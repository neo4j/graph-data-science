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
package org.neo4j.gds.catalog;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.RelationshipProjections;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.beta.generator.PropertyProducer;
import org.neo4j.gds.beta.generator.RandomGraphGeneratorBuilder;
import org.neo4j.gds.beta.generator.RelationshipDistribution;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

final class GraphInfoTest {

    @ParameterizedTest(name = "{1}")
    @MethodSource("producers")
    void shouldIncludeMemoryUsageUnlessRequested(PropertyProducer<?> producer, @SuppressWarnings("unused") String displayName) {
        assertThat(
            create(
                producer,
                (graphProjectConfig1, graphStore1) -> GraphInfo.withoutMemoryUsage(
                    graphProjectConfig1,
                    graphStore1
                )
            )
        )
            .returns(-1L, gi -> gi.sizeInBytes)
            .returns("", gi -> gi.memoryUsage);

        var graphInfo = create(
            producer,
            (graphProjectConfig, graphStore) -> GraphInfo.withMemoryUsage(graphProjectConfig, graphStore)
        );
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
            arguments(PropertyProducer.fixedDouble("singleValue", 42), "singleValueProducer"),
            arguments(
                PropertyProducer.randomEmbedding("lotsOfSmallValues", 42, 0.0F, 1.0F),
                "lotsOfSmallValuesProducer"
            )
        );
    }

    private GraphInfo create(
        PropertyProducer<?> nodePropertyProducer,
        BiFunction<GraphProjectConfig, GraphStore, GraphInfo> constructor
    ) {
        var graph = new RandomGraphGeneratorBuilder()
            .nodeCount(133_742)
            .averageDegree(1)
            .relationshipDistribution(RelationshipDistribution.UNIFORM)
            .nodePropertyProducer(nodePropertyProducer)
            .build()
            .generate();
        var storeConfig = ImmutableGraphProjectFromStoreConfig.builder()
            .graphName("foo")
            .nodeProjections(NodeProjections.ALL)
            .relationshipProjections(RelationshipProjections.ALL)
            .build();
        var graphStore = CSRGraphStoreUtil.createFromGraph(
            DatabaseId.from("test"),
            graph,
            Optional.empty(),
            1
        );

        return constructor.apply(storeConfig, graphStore);
    }
}
