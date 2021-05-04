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

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.condition.DisabledOnJre;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.NodeProjections;
import org.neo4j.graphalgo.RelationshipProjections;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.beta.generator.PropertyProducer;
import org.neo4j.graphalgo.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.graphalgo.beta.generator.RelationshipDistribution;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.ImmutableGraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.CSRGraphStoreUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.kernel.database.DatabaseIdFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.condition.JRE.JAVA_15;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@DisabledOnJre(JAVA_15)
final class GraphInfoTest {

    @ParameterizedTest(name = "{1}")
    @MethodSource("producers")
    void shouldIncludeMemoryUsageUnlessRequested(PropertyProducer<?> producer, @SuppressWarnings("unused") String displayName) {
        assertThat(create(producer, GraphInfo::withoutMemoryUsage))
            .returns(-1L, gi -> gi.sizeInBytes)
            .returns("", gi -> gi.memoryUsage);

        var graphInfo = create(producer, GraphInfo::withMemoryUsage);
        assertThat(graphInfo)
            .extracting(gi -> gi.sizeInBytes, as(InstanceOfAssertFactories.LONG))
            .isPositive();

        assertThat(graphInfo)
            .extracting(gi -> gi.memoryUsage, as(InstanceOfAssertFactories.STRING))
            .isNotBlank();
    }

    static Stream<Arguments> producers() {
        return Stream.of(
            arguments(new PropertyProducer.EmptyPropertyProducer(), "emptyProducer"),
            arguments(PropertyProducer.fixed("singleValue", 42), "singleValueProducer"),
            arguments(
                PropertyProducer.randomEmbeddings("lotsOfSmallValues", 42, 0.0F, 1.0F),
                "lotsOfSmallValuesProducer"
            )
        );
    }

    private GraphInfo create(
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

        return constructor.apply(storeConfig, graphStore);
    }
}
