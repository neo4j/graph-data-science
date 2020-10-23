/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.api.schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseTest;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GraphSchemaIntegrationTest extends BaseTest {

    private static String DB_CYPHER = "CREATE" +
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
    @MethodSource(value = "propertyMappingsAndExpectedResults")
    void computesCorrectSchema(NodePropertySchema expectedSchema, PropertyMapping propertyMapping) {
        Graph graph = new StoreLoaderBuilder()
            .api(db)
            .addNodeProjection(
                NodeProjection.of(
                    "Node",
                    PropertyMappings.of(List.of(propertyMapping))
                )
            )
            .build()
            .graph();

        assertEquals(expectedSchema, graph.schema().nodeSchema().properties().get(NodeLabel.of("Node")).get("prop"));
    }

    private static Stream<Arguments> propertyMappingsAndExpectedResults() {
        return Stream.of(
            Arguments.of(
                NodePropertySchema.of(ValueType.LONG),
                PropertyMapping.of("prop")
            ),
            Arguments.of(
                NodePropertySchema.of(ValueType.LONG, Optional.of(DefaultValue.of(1337)), Optional.empty()),
                PropertyMapping.of("prop", 1337)
            )
        );
    }
}
