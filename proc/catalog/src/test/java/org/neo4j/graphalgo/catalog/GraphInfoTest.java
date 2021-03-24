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
package org.neo4j.graphalgo.catalog;

import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.beta.generator.PropertyProducer;
import org.neo4j.graphalgo.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.CSRGraphStoreUtil;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.database.DatabaseIdFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

final class GraphInfoTest {

    @Test
    void shouldNotTakeLongToInstantiate() {
        // it's not super straight-forward to test this as we
        // don't want to measure against a fixed time.
        // We measure first how long it takes to create the info for a graph without properties
        // Then with single value and array value properties.
        // Those times should be close to each other.
        // To verify, we also measure how long it takes to create an instance with memory usage
        // and verify that those times are longer.

        var emptyProducer = new PropertyProducer.EmptyPropertyProducer();
        var singleValueProducer = PropertyProducer.fixed("singleValue", 42);
        var lotsOfSmallValuesProducer = PropertyProducer.randomEmbeddings("lotsOfSmallValues", 42, 0.0F, 1.0F);

        // do one warmup create
        millisToCreate(emptyProducer, GraphInfo::withoutMemoryUsage);
        var baseline = millisToCreate(emptyProducer, GraphInfo::withoutMemoryUsage);

        {
            var singleValue = millisToCreate(singleValueProducer, GraphInfo::withoutMemoryUsage);
            var lotsOfSmallValues = millisToCreate(lotsOfSmallValuesProducer, GraphInfo::withoutMemoryUsage);

            assertThat(singleValue).isCloseTo(baseline, within(10L));
            assertThat(lotsOfSmallValues).isCloseTo(baseline, within(10L));
        }

        {
            var singleValue = millisToCreate(singleValueProducer, GraphInfo::withMemoryUsage);
            var lotsOfSmallValues = millisToCreate(lotsOfSmallValuesProducer, GraphInfo::withMemoryUsage);

            assertThat(singleValue).isGreaterThan(baseline * 10);
            assertThat(lotsOfSmallValues).isGreaterThan(baseline * 10);
        }
    }


    @SuppressWarnings({"FieldCanBeLocal", "unused"})
    private volatile GraphInfo blackhole;

    private long millisToCreate(
        PropertyProducer<?> nodePropertyProducer,
        BiFunction<GraphCreateConfig, GraphStore, GraphInfo> constructor
    ) {
        var graph = new RandomGraphGeneratorBuilder()
            .nodeCount(133_742)
            .averageDegree(1)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(nodePropertyProducer)
            .build()
            .generate();
        var storeConfig = ImmutableGraphCreateFromStoreConfig.builder()
            .graphName("foo")
            .nodeProjections(NodeProjections.ALL)
            .relationshipProjections(RelationshipProjections.ALL)
            .build();
        var graphStore = CSRGraphStoreUtil.createFromGraph(
            DatabaseIdFactory.from("test", UUID.randomUUID()),
            graph,
            "TY",
            Optional.empty(),
            1,
            AllocationTracker.empty()
        );

        var timer = ProgressTimer.start();
        try (timer) {
            blackhole = constructor.apply(storeConfig, graphStore);
        }
        return timer.getDuration();
    }
}
