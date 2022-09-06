/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.gds.core.io.file;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.io.GraphStoreGraphPropertyVisitor;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

class GraphStoreGraphPropertyVisitorTest {

    @Test
    void shouldImportGraphProperties() throws IOException {
        var graphPropertySchema = Map.of(
            "prop1", PropertySchema.of("prop1", ValueType.LONG),
            "prop2", PropertySchema.of("prop2", ValueType.DOUBLE_ARRAY)
        );

        var graphPropertyVisitor = new GraphStoreGraphPropertyVisitor(graphPropertySchema);

        graphPropertyVisitor.property("prop1", 42L);
        graphPropertyVisitor.property("prop2", new double[]{ 13.37D, 42.0D });
        graphPropertyVisitor.property("prop1", 43L);
        graphPropertyVisitor.property("prop1", 44L);
        graphPropertyVisitor.property("prop2", new double[]{ 2.0, 3.0 });
        graphPropertyVisitor.flush();

        graphPropertyVisitor.streamFractions().forEach((key, streams) -> {
            assertThat(streams).hasSize(1);
        });

        var prop1Stream = graphPropertyVisitor.streamFractions().get("prop1");
        assertThat(prop1Stream).hasSize(1);
        var longStream = (LongStream) prop1Stream.get(0).build().stream();
        assertThat(longStream.toArray()).containsExactly(42L, 43L, 44L);

        var prop2Stream = graphPropertyVisitor.streamFractions().get("prop2");
        assertThat(prop2Stream).hasSize(1);
        var doubleArrayStream = (Stream<double[]>) prop2Stream.get(0).build().stream();
        assertThat(doubleArrayStream.collect(Collectors.toList())).containsExactly(
            new double[]{ 13.37D, 42.0D },
            new double[]{ 2.0, 3.0 }
        );
    }

    @Test
    void shouldImportGraphPropertiesConcurrent() throws InterruptedException {
        var graphPropertySchema = Map.of("prop1", PropertySchema.of("prop1", ValueType.LONG));
        var graphPropertyVisitor = new GraphStoreGraphPropertyVisitor(graphPropertySchema);

        var concurrency = 4;
        var periodicFlush = 100;
        var pool = Executors.newFixedThreadPool(concurrency);

        var phaser = new Phaser(concurrency + 1);
        IntStream
            .range(0, concurrency)
            .mapToObj(i -> new VisitTask(graphPropertyVisitor, i, phaser, periodicFlush))
            .forEach(pool::execute);

        phaser.arriveAndAwaitAdvance();

        pool.shutdown();
        pool.awaitTermination(10, TimeUnit.SECONDS);

        var streamFractions = graphPropertyVisitor.streamFractions().get("prop1");
        assertThat(streamFractions).hasSize(concurrency * periodicFlush);

        var longStream = (LongStream) streamFractions
            .stream()
            .map(GraphStoreGraphPropertyVisitor.StreamBuilder::build)
            .map(GraphStoreGraphPropertyVisitor.ReducibleStream::stream)
            .flatMapToLong(stream -> (LongStream) stream);
        var actual = longStream.boxed().collect(Collectors.toList());
        var expected = LongStream.range(0, 10_000 * concurrency).boxed().toArray(Long[]::new);

        assertThat(actual).containsExactlyInAnyOrder(expected);
    }

    @Test
    void closesThreadLocal() {
        var graphPropertySchema = Map.of("prop1", PropertySchema.of("prop1", ValueType.LONG));
        var graphPropertyVisitor = new GraphStoreGraphPropertyVisitor(graphPropertySchema);

        graphPropertyVisitor.close();

        CloseableThreadLocal<Random> threadLocal = null;
        try {
            //noinspection unchecked
            threadLocal = (CloseableThreadLocal<Random>) MethodHandles
                .privateLookupIn(GraphStoreGraphPropertyVisitor.class, MethodHandles.lookup())
                .findGetter(GraphStoreGraphPropertyVisitor.class, "streamBuilders", CloseableThreadLocal.class)
                .invoke(graphPropertyVisitor);
        } catch (Throwable e) {
            fail("couldn't inspect the field", e);
        }
        assertThatThrownBy(threadLocal::get).isInstanceOf(NullPointerException.class);
    }

    static class VisitTask implements Runnable {

        private final GraphStoreGraphPropertyVisitor graphStoreGraphPropertyVisitor;
        private final int offset;
        private final Phaser phaser;
        private final int periodicFlush;

        VisitTask(
            GraphStoreGraphPropertyVisitor graphStoreGraphPropertyVisitor,
            int offset,
            Phaser phaser,
            int periodicFlush
        ) {
            this.graphStoreGraphPropertyVisitor = graphStoreGraphPropertyVisitor;
            this.offset = offset;
            this.phaser = phaser;
            this.periodicFlush = periodicFlush;
        }

        @Override
        public void run() {
            phaser.arriveAndAwaitAdvance();
            try {
                for (int i = 0; i < 10_000; i++) {
                    graphStoreGraphPropertyVisitor.property("prop1", (long) i + (offset * 10_000L));
                    // call flush periodically to increase the potential contention
                    if (i > 0 && i % periodicFlush == 0) {
                        graphStoreGraphPropertyVisitor.flush();
                    }
                }
                graphStoreGraphPropertyVisitor.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
