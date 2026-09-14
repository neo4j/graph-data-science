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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.PropertyMapping.Key;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.legacycypherprojection.GraphProjectFromCypherConfig;
import org.neo4j.gds.legacycypherprojection.GraphProjectFromCypherConfigImpl;
import org.neo4j.gds.projection.GraphProjectFromStoreConfig;
import org.neo4j.gds.projection.GraphProjectFromStoreConfigImpl;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.gds.NodeLabel.ALL_NODES;
import static org.neo4j.gds.RelationshipType.ALL_RELATIONSHIPS;
import static org.neo4j.gds.legacycypherprojection.GraphProjectFromCypherConfig.ALL_NODES_QUERY;
import static org.neo4j.gds.legacycypherprojection.GraphProjectFromCypherConfig.ALL_RELATIONSHIPS_QUERY;

class GraphProjectConfigBuildersTest {

    private static final JobId jobId = new JobId();

    static Stream<Arguments> storeConfigs() {
        return Stream.of(
            Arguments.arguments(
                new StoreConfigBuilder().userName("foo").graphName("bar").jobId(jobId).build(),
                GraphProjectFromStoreConfigImpl.builder().username("foo").graphName("bar")
                    .nodeProjections(NodeProjections.all())
                    .relationshipProjections(RelationshipProjections.ALL)
                    .nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder().jobId(jobId).build(),
                GraphProjectFromStoreConfigImpl.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.single(ALL_NODES, NodeProjection.all()))
                    .relationshipProjections(new RelationshipProjections(
                        Map.of(ALL_RELATIONSHIPS, RelationshipProjection.ALL)
                    )).nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder().addNodeLabel("Foo").addRelationshipType("BAR").jobId(jobId).build(),
                GraphProjectFromStoreConfigImpl.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.single(NodeLabel.of("Foo"), new NodeProjection("Foo")))
                    .relationshipProjections(new RelationshipProjections(
                        Map.of(
                            RelationshipType.of("BAR"),
                            new RelationshipProjection("BAR", Orientation.NATURAL, Aggregation.DEFAULT)
                        )
                    )).nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder()
                    .addNodeProjection(new NodeProjection("Foo"))
                    .addRelationshipType("BAR")
                    .jobId(jobId)
                    .build(),
                GraphProjectFromStoreConfigImpl.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.single(NodeLabel.of("Foo"), new NodeProjection("Foo")))
                    .relationshipProjections(new RelationshipProjections(
                        Map.of(
                            RelationshipType.of("BAR"),
                            new RelationshipProjection("BAR", Orientation.NATURAL, Aggregation.DEFAULT)
                        )
                    )).nodeProperties(PropertyMappings.of())
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
                GraphProjectFromStoreConfigImpl.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.single(NodeLabel.of("Foo"), new NodeProjection("Foo")))
                    .relationshipProjections(new RelationshipProjections(
                        Map.of(
                            RelationshipType.of("BAR"),
                            new RelationshipProjection("BAR", Orientation.UNDIRECTED, Aggregation.DEFAULT)
                        )
                    )).nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder()
                    .addNodeLabel("Foo")
                    .addRelationshipType("BAR")
                    .addRelationshipProjection(new RelationshipProjection("BAZ", Orientation.NATURAL))
                    .globalProjection(Orientation.UNDIRECTED)
                    .jobId(jobId)
                    .build(),
                GraphProjectFromStoreConfigImpl.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.single(NodeLabel.of("Foo"), new NodeProjection("Foo")))
                    .relationshipProjections(new RelationshipProjections(
                        Map.of(
                            RelationshipType.of("BAR"),
                            new RelationshipProjection("BAR", Orientation.UNDIRECTED, Aggregation.DEFAULT),
                            RelationshipType.of("BAZ"),
                            new RelationshipProjection("BAZ", Orientation.NATURAL, Aggregation.DEFAULT)
                        )
                    ))
                    .nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new StoreConfigBuilder()
                    .addNodeLabel("Foo")
                    .addRelationshipType("BAR")
                    .nodeProperties(Collections.singletonList(
                        PropertyMapping.of(Key.simple("nProp"), DefaultValue.of(23.0D)))
                    ).relationshipProperties(Collections.singletonList(
                        PropertyMapping.of(Key.simple("rProp"), DefaultValue.of(42.0D)))
                    ).jobId(jobId)
                    .build(),
                GraphProjectFromStoreConfigImpl.builder().username("").graphName("")
                    .nodeProjections(NodeProjections.single(
                            NodeLabel.of("Foo"),
                            new NodeProjection(
                                "Foo",
                                PropertyMappings.of(
                                    PropertyMapping.of(Key.simple("nProp"), DefaultValue.of(23.0D))
                                )
                            )
                        ))
                    .relationshipProjections(new RelationshipProjections(
                        Map.of(
                            RelationshipType.of("BAR"),
                            new RelationshipProjection(
                                "BAR",
                                PropertyMappings.of(
                                    PropertyMapping.of(Key.simple("rProp"), DefaultValue.of(42.0D))
                                )
                            )
                        )
                    )).nodeProperties(PropertyMappings.of())
                    .relationshipProperties(PropertyMappings.of())
                    .jobId(jobId)
                    .build()
            )
        );
    }

    @Disabled("Add equality to config implementations or rewrite test")
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
                GraphProjectFromCypherConfigImpl.builder().username("").graphName("")
                    .nodeQuery(ALL_NODES_QUERY)
                    .relationshipQuery(ALL_RELATIONSHIPS_QUERY)
                    .jobId(jobId)
                    .build()
            ),
            Arguments.arguments(
                new CypherConfigBuilder().userName("foo").graphName("bar")
                    .jobId(jobId)
                    .build(),
                GraphProjectFromCypherConfigImpl.builder().username("foo").graphName("bar")
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
                GraphProjectFromCypherConfigImpl.builder().username("foo").graphName("bar")
                    .nodeQuery("MATCH (n:Foo) RETURN id(n) AS id")
                    .relationshipQuery("MATCH (a)-->(b) RETURN id(a) AS source, id(b) AS target")
                    .jobId(jobId)
                    .build()
            )
        );
    }

    @Disabled("Add equality to config implementations or rewrite test")
    @ParameterizedTest
    @MethodSource("cypherConfigs")
    void testCypherConfigBuilder(GraphProjectFromCypherConfig actual, GraphProjectFromCypherConfig expected) {
        assertEquals(expected, actual);
    }

}
