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
package org.neo4j.gds.core.io.json;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

@GdlExtension
class GraphStoreMetadataFactoryTest {

    @GdlGraph
    public static final String GDL = """
            (a:Label1 {prop1: 42, prop2: 4.2}),
            (b:Label2 {prop3: [1L, 3L, 3L, 7L], prop4: [1.0f, 3.0f, 3.0f, 7.0f]}),
            (c:Label3),
            (d:Label3),
            (a)-[:REL_TYPE1 {relProp1: 13.37}]->(b),
            (b)-[:REL_TYPE1 {relProp1: 1.337}]->(c),
            (a)-[:REL_TYPE2]->(c)
            (a)-[:REL_TYPE2]->(d)
        """;

    @Inject
    private GraphStore graphStore;

    @Test
    void fromGraphStore() {
        var result = GraphStoreMetadataFactory.fromGraphStore(graphStore);

        assertThat(result).isEqualTo(new GraphStoreMetadata(
                new DatabaseInfo("gdl", DatabaseInfo.DatabaseLocation.LOCAL, Optional.empty()),
                WriteMode.LOCAL,
                new IdMapInfo(
                    "array",
                    4,
                    1340,
                    Map.of(
                        "Label1", 1L,
                        "Label2", 1L,
                        "Label3", 2L
                    )
                ),
                Map.of(
                    "REL_TYPE1", new RelationshipInfo(2, false, 1),
                    "REL_TYPE2", new RelationshipInfo(2, false, 1)
                ),
                Map.of(
                    "Label1", new NodeSchema(Map.of(
                        "prop1", new NodePropertySchema(ValueType.LONG, Long.MIN_VALUE, PropertyState.TRANSIENT),
                        "prop2", new NodePropertySchema(ValueType.DOUBLE, Double.NaN, PropertyState.TRANSIENT)
                    )),
                    "Label2", new NodeSchema(Map.of(
                        "prop3", new NodePropertySchema(ValueType.LONG_ARRAY, null, PropertyState.TRANSIENT),
                        "prop4", new NodePropertySchema(ValueType.FLOAT_ARRAY, null, PropertyState.TRANSIENT)
                    )),
                    "Label3", new NodeSchema(Map.of())
                ),
                Map.of(
                    "REL_TYPE1", new RelationshipSchema(
                        Direction.DIRECTED,
                        Map.of(
                            "relProp1",
                            new RelationshipPropertySchema(
                                ValueType.DOUBLE,
                                Double.NaN,
                                PropertyState.TRANSIENT,
                                Aggregation.NONE
                            )
                        )
                    ),
                    "REL_TYPE2", new RelationshipSchema(Direction.DIRECTED, Map.of())
                )
            )
        );
    }

    @Test
    void toDatabaseLocation() {
        assertThat(GraphStoreMetadataFactory.toDatabaseLocation(org.neo4j.gds.api.DatabaseInfo.DatabaseLocation.LOCAL))
            .isEqualTo(DatabaseInfo.DatabaseLocation.LOCAL);
        assertThat(GraphStoreMetadataFactory.toDatabaseLocation(org.neo4j.gds.api.DatabaseInfo.DatabaseLocation.REMOTE))
            .isEqualTo(DatabaseInfo.DatabaseLocation.REMOTE);
        assertThat(GraphStoreMetadataFactory.toDatabaseLocation(org.neo4j.gds.api.DatabaseInfo.DatabaseLocation.NONE))
            .isEqualTo(DatabaseInfo.DatabaseLocation.NONE);
    }

    @Test
    void testToWriteMode() {
        assertThat(GraphStoreMetadataFactory.toWriteMode(Capabilities.WriteMode.LOCAL))
            .isEqualTo(WriteMode.LOCAL);
        assertThat(GraphStoreMetadataFactory.toWriteMode(Capabilities.WriteMode.REMOTE))
            .isEqualTo(WriteMode.REMOTE);
    }

    @Test
    void toIdMapInfo() {
        var result = GraphStoreMetadataFactory.toIdMapInfo(graphStore.nodes());

        assertThat(result).isEqualTo(new IdMapInfo(
            "array",
            4,
            1340,
            Map.of(
                "Label1", 1L,
                "Label2", 1L,
                "Label3", 2L
            )
        ));
    }

    @Test
    void toRelationshipInfo() {
        var result = GraphStoreMetadataFactory.toRelationshipInfo(graphStore);

        assertThat(result).isEqualTo(Map.of(
            "REL_TYPE1", new RelationshipInfo(2, false, 1),
            "REL_TYPE2", new RelationshipInfo(2, false, 1)
        ));
    }

    @Test
    void toNodeSchema() {
        var result = GraphStoreMetadataFactory.toNodeSchema(graphStore.schema().nodeSchema());

        assertThat(result).isEqualTo(Map.of(
            "Label1", new NodeSchema(Map.of(
                "prop1", new NodePropertySchema(ValueType.LONG, Long.MIN_VALUE, PropertyState.TRANSIENT),
                "prop2", new NodePropertySchema(ValueType.DOUBLE, Double.NaN, PropertyState.TRANSIENT)
            )),
            "Label2", new NodeSchema(Map.of(
                "prop3", new NodePropertySchema(ValueType.LONG_ARRAY, null, PropertyState.TRANSIENT),
                "prop4", new NodePropertySchema(ValueType.FLOAT_ARRAY, null, PropertyState.TRANSIENT)
            )),
            "Label3", new NodeSchema(Map.of())
        ));
    }

    @Test
    void toRelationshipSchema() {
        var result = GraphStoreMetadataFactory.toRelationshipSchema(graphStore.schema().relationshipSchema());

        assertThat(result).isEqualTo(Map.of(
            "REL_TYPE1", new RelationshipSchema(
                Direction.DIRECTED,
                Map.of(
                    "relProp1",
                    new RelationshipPropertySchema(
                        ValueType.DOUBLE,
                        Double.NaN,
                        PropertyState.TRANSIENT,
                        Aggregation.NONE
                    )
                )
            ),
            "REL_TYPE2", new RelationshipSchema(Direction.DIRECTED, Map.of())
        ));
    }

    static Stream<Arguments> nodeSchemas() {
        return Stream.of(
            Arguments.of(
                // empty
                MutableNodeSchema.empty(),
                Map.of()
            ),
            Arguments.of(
                // all nodes label, no properties
                MutableNodeSchema.empty().addLabel(NodeLabel.ALL_NODES),
                Map.of(NodeLabel.ALL_NODES.name(), new NodeSchema(Map.of()))
            ),
            Arguments.of(
                // no label, with properties
                MutableNodeSchema.empty()
                    .addLabel(
                        NodeLabel.ALL_NODES,
                        Map.of("foo", PropertySchema.of("foo", org.neo4j.gds.api.nodeproperties.ValueType.LONG))
                    ),
                Map.of(
                    NodeLabel.ALL_NODES.name(),
                    new NodeSchema(Map.of(
                        "foo",
                        new NodePropertySchema(
                            ValueType.LONG,
                            DefaultValue.forLong().getObject(),
                            PropertyState.PERSISTENT
                        )
                    ))
                )
            ),
            Arguments.of(
                // label and properties
                MutableNodeSchema.empty()
                    .addLabel(
                        NodeLabel.of("A"),
                        Map.of("foo", PropertySchema.of("foo", org.neo4j.gds.api.nodeproperties.ValueType.LONG))
                    )
                    .addLabel(
                        NodeLabel.of("B"),
                        Map.of(
                            "bar", PropertySchema.of(
                                "bar",
                                org.neo4j.gds.api.nodeproperties.ValueType.LONG, DefaultValue.of(42L),
                                org.neo4j.gds.api.PropertyState.REMOTE
                            )
                        )
                    ),
                Map.of(
                    "A",
                    new NodeSchema(Map.of(
                        "foo",
                        new NodePropertySchema(
                            ValueType.LONG,
                            DefaultValue.forLong().getObject(),
                            PropertyState.PERSISTENT
                        )
                    )),
                    "B",
                    new NodeSchema(Map.of(
                        "bar",
                        new NodePropertySchema(
                            ValueType.LONG,
                            42L,
                            PropertyState.REMOTE
                        )
                    ))
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("nodeSchemas")
    void toNodeSchema(org.neo4j.gds.api.schema.NodeSchema input, Map<String, NodeSchema> expected) {
        assertThat(GraphStoreMetadataFactory.toNodeSchema(input)).isEqualTo(expected);
    }

    static Stream<Arguments> relationshipSchemas() {
        return Stream.of(
            Arguments.of(
                // empty
                org.neo4j.gds.api.schema.MutableRelationshipSchema.empty(),
                Map.of()
            ),
            Arguments.of(
                // all relationship type, no properties
                org.neo4j.gds.api.schema.MutableRelationshipSchema.empty()
                    .addRelationshipType(
                        RelationshipType.ALL_RELATIONSHIPS,
                        org.neo4j.gds.api.schema.Direction.DIRECTED
                    ),
                Map.of(
                    RelationshipType.ALL_RELATIONSHIPS.name(),
                    new RelationshipSchema(Direction.DIRECTED, Map.of())
                )
            ),
            Arguments.of(
                // relationship type and properties
                org.neo4j.gds.api.schema.MutableRelationshipSchema.empty()
                    .addProperty(
                        RelationshipType.of("REL"),
                        org.neo4j.gds.api.schema.Direction.UNDIRECTED,
                        "relProp",
                        org.neo4j.gds.api.nodeproperties.ValueType.DOUBLE,
                        org.neo4j.gds.api.PropertyState.TRANSIENT
                    ),
                Map.of(
                    "REL",
                    new RelationshipSchema(
                        Direction.UNDIRECTED,
                        Map.of(
                            "relProp",
                            new RelationshipPropertySchema(
                                ValueType.DOUBLE,
                                Double.NaN,
                                PropertyState.TRANSIENT,
                                Aggregation.NONE
                            )
                        )
                    )
                )
            )
        );
    }

    @ParameterizedTest
    @MethodSource("relationshipSchemas")
    void toRelationshipSchema(
        org.neo4j.gds.api.schema.RelationshipSchema input,
        Map<String, RelationshipSchema> expected
    ) {
        assertThat(GraphStoreMetadataFactory.toRelationshipSchema(input)).isEqualTo(expected);
    }
}
