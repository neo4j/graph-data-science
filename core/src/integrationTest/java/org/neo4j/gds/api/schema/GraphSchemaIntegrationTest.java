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
package org.neo4j.gds.api.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.BaseTest;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.StoreLoaderBuilder;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.core.Aggregation;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GraphSchemaIntegrationTest extends BaseTest {

    private static final String DB_CYPHER = "CREATE" +
                                            "  (a:Node {prop: 1})" +
                                            ", (b:Node {prop: 2})" +
                                            ", (c:Node)" +
                                            ", (a)-[:REL {relProp: 42.0}]->(b)" +
                                            ", (b)-[:REL]->(c)";

    @BeforeEach
    void setup() {
        runQuery(DB_CYPHER);
    }

    @ParameterizedTest
    @MethodSource(value = "nodePropertyMappingsAndExpectedResults")
    void computesCorrectNodeSchema(PropertySchema expectedSchema, PropertyMapping propertyMapping) {
        Graph graph = new StoreLoaderBuilder()
            .databaseService(db)
            .addNodeProjection(
                NodeProjection.builder()
                    .label("Node")
                    .addProperty(propertyMapping)
                    .build()
            )
            .build()
            .graph();

        assertEquals(expectedSchema, graph.schema().nodeSchema().get(NodeLabel.of("Node")).properties().get("prop"));
    }

    @ParameterizedTest
    @MethodSource(value = "relationshipPropertyMappingsAndExpectedResults")
    void computesCorrectRelationshipSchema(RelationshipPropertySchema expectedSchema, PropertyMapping propertyMapping) {
        Graph graph = new StoreLoaderBuilder()
            .databaseService(db)
            .addRelationshipProjection(
                RelationshipProjection.builder()
                    .type("REL")
                    .addProperties(propertyMapping)
                    .build()
            )
            .build()
            .graph();

        assertEquals(expectedSchema, graph.schema().relationshipSchema().get(RelationshipType.of("REL")).properties().get("relProp"));
    }

    private static Stream<Arguments> nodePropertyMappingsAndExpectedResults() {
        return Stream.of(
            Arguments.of(
                PropertySchema.of("prop", ValueType.LONG),
                PropertyMapping.of("prop")
            ),
            Arguments.of(
                PropertySchema.of("prop", ValueType.LONG, DefaultValue.of(1337), PropertyState.PERSISTENT),
                PropertyMapping.of("prop", 1337)
            )
        );
    }

    private static Stream<Arguments> relationshipPropertyMappingsAndExpectedResults() {
        return Stream.of(
            Arguments.of(
                RelationshipPropertySchema.of("relProp", ValueType.DOUBLE),
                PropertyMapping.of("relProp")
            ),
            Arguments.of(
                RelationshipPropertySchema.of(
                    "relProp",
                    ValueType.DOUBLE,
                    DefaultValue.of(1337.0D),
                    PropertyState.PERSISTENT,
                    Aggregation.MAX
                ),
                PropertyMapping.of("relProp", DefaultValue.of(1337.0D), Aggregation.MAX)
            )
        );
    }
}
