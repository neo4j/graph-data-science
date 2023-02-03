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
package org.neo4j.gds.beta.k1coloring;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.TestProgressTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.beta.generator.RandomGraphGenerator;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.config.RandomGraphGeneratorConfig.AllowSelfLoops;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class K1ColoringTest {

    @Test
    void testK1Coloring() {
        final String DB_CYPHER =
            "CREATE" +
            " (a)" +
            ",(b)" +
            ",(c)" +
            ",(d)" +
            ",(a)-[:REL]->(b)" +
            ",(a)-[:REL]->(c)";

        var graph = fromGdl(DB_CYPHER);

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            1000,
            DEFAULT_BATCH_SIZE,
            1,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        k1Coloring.compute();

        HugeLongArray colors = k1Coloring.colors();

        var colorOfNode0 = colors.get(0);
        var colorOfNode1 = colors.get(1);
        var colorOfNode2 = colors.get(2);

        assertThat(colorOfNode0)
            .as("Color of Node0 should be unique")
            .isNotEqualTo(colorOfNode1)
            .isNotEqualTo(colorOfNode2);
        assertThat(colorOfNode1)
            .as("Color of Node1 should be the same as color of Node2")
            .isEqualTo(colorOfNode2);
    }

    @Test
    void testParallelK1Coloring() {
        long seed = 42L;

        var graph = RandomGraphGenerator.builder()
            .nodeCount(200_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.POWER_LAW)
            .seed(seed)
            .aggregation(Aggregation.NONE)
            .direction(Direction.UNDIRECTED)
            .allowSelfLoops(AllowSelfLoops.NO)
            .build()
            .generate();

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            100,
            DEFAULT_BATCH_SIZE,
            8,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        k1Coloring.compute();
        HugeLongArray colors = k1Coloring.colors();

        var usedColors = new HashSet<>(100);
        var conflicts = new MutableLong(0);
        graph.forEachNode((nodeId) -> {
            graph.forEachRelationship(nodeId, (source, target) -> {
                if (source != target && colors.get(source) == colors.get(target)) {
                    conflicts.increment();
                }
                usedColors.add(colors.get(source));
                return true;
            });
            return true;
        });

        assertThat(conflicts.longValue())
            .as("Conflicts should be less than 20")
            .isLessThan(20L);

        assertThat(usedColors.size())
            .as("Used colors should be less than or equal to 21")
            .isLessThanOrEqualTo(21);
    }


    @Test
    void shouldComputeMemoryEstimation1Thread() {
        long nodeCount = 100_000L;
        int concurrency = 1;

        assertMemoryEstimation(nodeCount, concurrency, 825248);
    }

    @Test
    void shouldComputeMemoryEstimation4Threads() {
        long nodeCount = 100_000L;
        int concurrency = 4;
        assertMemoryEstimation(nodeCount, concurrency, 863000);
    }

    @Test
    void shouldComputeMemoryEstimation42Threads() {
        long nodeCount = 100_000L;
        int concurrency = 42;
        assertMemoryEstimation(nodeCount, concurrency, 1341192);
    }

    @Test
    void everyNodeShouldHaveBeenColored() {
        RandomGraphGenerator generator = RandomGraphGenerator.builder()
            .nodeCount(100_000)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .build();

        Graph graph = generator.generate();

        K1Coloring k1Coloring = new K1Coloring(
            graph,
            100,
            DEFAULT_BATCH_SIZE,
            8,
            Pools.DEFAULT,
            ProgressTracker.NULL_TRACKER
        );

        k1Coloring.compute();

        assertThat(k1Coloring.usedColors().get(ColoringStep.INITIAL_FORBIDDEN_COLORS))
            .as("The result should not contain the initial forbidden colors")
            .isFalse();
    }

    @Test
    void shouldLogProgress(){
        var graph = RandomGraphGenerator.builder()
            .nodeCount(100)
            .averageDegree(10)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .seed(42L)
            .build()
            .generate();

        var concurrency = 4;

        var config = ImmutableK1ColoringStreamConfig.builder()
            .concurrency(concurrency)
            .maxIterations(10)
            .build();

        var progressTask = new K1ColoringFactory<>().progressTask(graph, config);
        var log = Neo4jProxy.testLog();
        var progressTracker = new TestProgressTracker(progressTask, log, concurrency, EmptyTaskRegistryFactory.INSTANCE);

        var k1Coloring = new K1Coloring(
            graph,
            config.maxIterations(),
            DEFAULT_BATCH_SIZE,
            config.concurrency(),
            Pools.DEFAULT,
            progressTracker
        );

        k1Coloring.compute();

        List<AtomicLong> progresses = progressTracker.getProgresses();

        assertEquals(k1Coloring.ranIterations() * concurrency + 1, progresses.size());
        progresses.forEach(progress -> assertTrue(progress.get() <= 2 * graph.relationshipCount()));

        assertTrue(log.containsMessage(TestLog.INFO, ":: Start"));
        LongStream.range(1, k1Coloring.ranIterations() + 1).forEach(iteration ->
            assertThat(log.getMessages(TestLog.INFO)).anyMatch(message -> {
                var expected = formatWithLocale("%d of %d", iteration, config.maxIterations());
                return message.contains(expected);
            })
        );
        assertTrue(log.containsMessage(TestLog.INFO, ":: Finished"));
    }

    private void assertMemoryEstimation(long nodeCount, int concurrency, long expected) {
        GraphDimensions dimensions = ImmutableGraphDimensions.builder().nodeCount(nodeCount).build();
        K1ColoringStreamConfig config = ImmutableK1ColoringStreamConfig.builder().build();
        final MemoryRange actual = new K1ColoringFactory<>()
            .memoryEstimation(config)
            .estimate(dimensions, concurrency)
            .memoryUsage();

        assertEquals(actual.min, actual.max);
        assertEquals(expected, actual.min);
    }

}
