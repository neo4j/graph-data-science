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
package org.neo4j.gds;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.config.GraphProjectFromCypherConfig;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromCypherConfig;
import org.neo4j.gds.config.ImmutableGraphProjectFromStoreConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.utils.progress.JobId;

import java.util.Collections;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.gds.config.GraphProjectFromCypherConfig.ALL_RELATIONSHIPS_QUERY;

class GraphProjectConfigBuildersTest {

    private static final JobId jobId = new JobId();

    static Stream<Arguments> storeConfigs() {
        return Stream.of(
            Arguments.arguments(
                new StoreConfigBuilder().userName("foo").graphName("bar").jobId(jobId).build(),
                ImmutableGraphProjectFromStoreConfig.builder().username("foo").graphName("bar")
                    .nodeProjections(NodeProjections.all())
                    .relationshipProjections(AbstractRelationshipProjections.ALL)
                    .nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder().jobId(jobId).build(),
                ImmutableGraphProjectFromStoreConfig.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.builder()
                        .putProjection(ALL_NODES, NodeProjection.all())
                        .build())
                    .relationshipProjections(RelationshipProjections.builder()
                        .putProjection(ALL_RELATIONSHIPS, RelationshipProjection.ALL)
                        .build())
                    .nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder().addNodeLabel("Foo").addRelationshipType("BAR").jobId(jobId).build(),
                ImmutableGraphProjectFromStoreConfig.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.builder()
                        .putProjection(NodeLabel.of("Foo"), NodeProjection.of("Foo", PropertyMappings.of()))
                        .build())
                    .relationshipProjections(RelationshipProjections.builder()
                        .putProjection(
                            RelationshipType.of("BAR"),
                            RelationshipProjection.of("BAR", Orientation.NATURAL, Aggregation.DEFAULT)
                        )
                        .build())
                    .nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder()
                    .addNodeProjection(NodeProjection.fromString("Foo"))
                    .addRelationshipType("BAR")
                    .jobId(jobId)
                    .build(),
                ImmutableGraphProjectFromStoreConfig.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.builder()
                        .putProjection(NodeLabel.of("Foo"), NodeProjection.of("Foo", PropertyMappings.of()))
                        .build())
                    .relationshipProjections(RelationshipProjections.builder()
                        .putProjection(
                            RelationshipType.of("BAR"),
                            RelationshipProjection.of("BAR", Orientation.NATURAL, Aggregation.DEFAULT)
                        )
                        .build())
                    .nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder()
                    .addNodeLabel("Foo")
                    .addRelationshipType("BAR")
                    .globalProjection(Orientation.UNDIRECTED)
                    .jobId(jobId)
                    .build(),
                ImmutableGraphProjectFromStoreConfig.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.builder()
                        .putProjection(NodeLabel.of("Foo"), NodeProjection.of("Foo", PropertyMappings.of()))
                        .build())
                    .relationshipProjections(RelationshipProjections.builder()
                        .putProjection(
                            RelationshipType.of("BAR"),
                            RelationshipProjection.of("BAR", Orientation.UNDIRECTED, Aggregation.DEFAULT)
                        )
                        .build())
                    .nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder()
                    .addNodeLabel("Foo")
                    .addRelationshipType("BAR")
                    .addRelationshipProjection(RelationshipProjection.of("BAZ", Orientation.NATURAL))
                    .globalProjection(Orientation.UNDIRECTED)
                    .jobId(jobId)
                    .build(),
                ImmutableGraphProjectFromStoreConfig.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.builder()
                        .putProjection(NodeLabel.of("Foo"), NodeProjection.of("Foo", PropertyMappings.of()))
                        .build())
                    .relationshipProjections(RelationshipProjections.builder()
                        .putProjection(
                            RelationshipType.of("BAR"),
                            RelationshipProjection.of("BAR", Orientation.UNDIRECTED, Aggregation.DEFAULT)
                        )
                        .putProjection(
                            RelationshipType.of("BAZ"),
                            RelationshipProjection.of("BAZ", Orientation.NATURAL, Aggregation.DEFAULT)
                        )
                        .build())
                    .nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder()
                    .addNodeLabel("Foo")
                    .addRelationshipType("BAR")
                    .nodeProperties(Collections.singletonList(PropertyMapping.of("nProp", DefaultValue.of(23.0D))))
                    .relationshipProperties(Collections.singletonList(PropertyMapping.of(
                        "rProp",
                        DefaultValue.of(42.0D)
                    )))
                    .jobId(jobId)
                    .build(),
                ImmutableGraphProjectFromStoreConfig.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.builder()
                        .putProjection(
                            NodeLabel.of("Foo"),
                            NodeProjection.builder()
                                .label("Foo")
                                .addProperty(PropertyMapping.of("nProp", DefaultValue.of(23.0D)))
                                .build()
                        )
                        .build())
                    .relationshipProjections(RelationshipProjections.builder()
                        .putProjection(
                            RelationshipType.of("BAR"),
                            RelationshipProjection.builder()
                                .type("BAR")
                                .addProperty(PropertyMapping.of("rProp", DefaultValue.of(42.0D)))
                                .build()
                        )
                        .build())
                    .nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            )
        );
    }

    @ParameterizedTest
    @MethodSource("storeConfigs")
    void testStoreConfigBuilder(GraphProjectFromStoreConfig actual, GraphProjectFromStoreConfig expected) {
        assertEquals(expected, actual);
    }

    static Stream<Arguments> cypherConfigs() {
        return Stream.of(
            Arguments.arguments(
                new CypherConfigBuilder()
                    .jobId(jobId)
                    .build(),
                ImmutableGraphProjectFromCypherConfig.builder().username("").graphName("")
                    .nodeQuery(ALL_NODES_QUERY)
                    .relationshipQuery(ALL_RELATIONSHIPS_QUERY)
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new CypherConfigBuilder().userName("foo").graphName("bar")
                    .jobId(jobId)
                    .build(),
                ImmutableGraphProjectFromCypherConfig.builder().username("foo").graphName("bar")
                    .nodeQuery(ALL_NODES_QUERY)
                    .relationshipQuery(ALL_RELATIONSHIPS_QUERY)
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new CypherConfigBuilder().userName("foo").graphName("bar")
                    .nodeQuery("MATCH (n:Foo) RETURN id(n) AS id")
                    .relationshipQuery("MATCH (a)-->(b) RETURN id(a) AS source, id(b) AS target")
                    .jobId(jobId)
                    .build(),
                ImmutableGraphProjectFromCypherConfig.builder().username("foo").graphName("bar")
                    .nodeQuery("MATCH (n:Foo) RETURN id(n) AS id")
                    .relationshipQuery("MATCH (a)-->(b) RETURN id(a) AS source, id(b) AS target")
                    .jobId(jobId)
                    .build()
            )
        );
    }

    @ParameterizedTest
    @MethodSource("cypherConfigs")
    void testCypherConfigBuilder(GraphProjectFromCypherConfig actual, GraphProjectFromCypherConfig expected) {
        assertEquals(expected, actual);
    }

}
