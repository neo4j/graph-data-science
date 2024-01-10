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
package org.neo4j.gds.procedures.catalog;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
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
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.CSRGraphStoreUtil;
import org.neo4j.gds.projection.GraphProjectFromStoreConfig;
import org.neo4j.gds.projection.GraphProjectFromStoreConfigImpl;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

final class GraphInfoTest {

    @ParameterizedTest(name = "{1}")
    @MethodSource("producers")
    void shouldIncludeMemoryUsageUnlessRequested(
        PropertyProducer<?> producer,
        @SuppressWarnings("unused") String displayName
    ) {
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
            arguments(PropertyProducer.fixedDouble("singleDoubleValue", 42), "singleDoubleValueProducer"),
            arguments(PropertyProducer.fixedLong("singleLongValue", 42), "singleLongValueProducer"),
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
        var storeConfig = GraphProjectFromStoreConfigImpl.builder()
            .username("")
            .graphName("foo")
            .nodeProjections(NodeProjections.ALL)
            .relationshipProjections(RelationshipProjections.ALL)
            .build();
        var graphStore = CSRGraphStoreUtil.createFromGraph(
            DatabaseId.of("test"),
            graph,
            Optional.empty(),
            1
        );

        return constructor.apply(storeConfig, graphStore);
    }

    /**
     * What is this you ask?
     * <p>
     * Well we used to have test of the drop procedure that asserted about these things,
     * and the marshalling code for {configuration, store}->graph info is a bit convoluted.
     * So for an abundance of caution I captured this.
     */
    @Test
    void shouldConstructFromStoreWithoutMemoryUsage() {
        var creationTime = ZonedDateTime.of(1969, 7, 20, 20, 17, 40, 0, ZoneId.of("UTC"));
        var modificationTime = ZonedDateTime.of(1963, 8, 28, 17, 0, 0, 0, ZoneId.of("GMT-5"));
        var graphProjectConfig = GraphProjectFromStoreConfig.of(
            "some user",
            "some graph",
            "A",
            "REL",
            CypherMapWrapper.create(
                Map.of(
                    "creationTime", creationTime,
                    "jobId", "some job"
                )
            )
        );
        var graphStore = new DummyGraphStore(modificationTime);
        var graphInfo = GraphInfo.withoutMemoryUsage(graphProjectConfig, graphStore);

        assertThat(graphInfo.creationTime).isEqualTo(creationTime);
        assertThat(graphInfo.configuration).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "creationTime", creationTime,
                "jobId", "some job",
                "logProgress", true,
                "nodeProjection", Map.of(
                    "A", Map.of(
                        "label", "A",
                        "properties", emptyMap()
                    )
                ),
                "nodeProperties", emptyMap(),
                "readConcurrency", 4,
                "relationshipProjection", Map.of(
                    "REL", Map.of(
                        "type", "REL",
                        "orientation", "NATURAL",
                        "aggregation", "DEFAULT",
                        "indexInverse", false,
                        "properties", emptyMap()
                    )
                ),
                "relationshipProperties", emptyMap(),
                "sudo", false,
                "validateRelationships", false
            )
        );
        assertThat(graphInfo.database).isEqualTo("some database");
        assertThat(graphInfo.density).isEqualTo(0.5);
        assertThat(graphInfo.graphName).isEqualTo("some graph");
        assertThat(graphInfo.memoryUsage).isEqualTo("");
        assertThat(graphInfo.modificationTime).isEqualTo(modificationTime);
        assertThat(graphInfo.nodeCount).isEqualTo(2L);
        assertThat(graphInfo.relationshipCount).isEqualTo(1L);
        assertThat(graphInfo.schema).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "nodes", Map.of("A", Map.of()),
                "relationships", Map.of("REL", Map.of()),
                "graphProperties", Map.of()
            )
        );
        assertThat(graphInfo.schemaWithOrientation).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "nodes", Map.of("A", Map.of()),
                "relationships", Map.of("REL", Map.of("direction", "DIRECTED", "properties", Map.of())),
                "graphProperties", Map.of()
            )
        );
        assertThat(graphInfo.sizeInBytes).isEqualTo(-1L);
    }
}
