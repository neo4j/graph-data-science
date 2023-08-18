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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphStreamNodePropertiesProcTest extends BaseProcTest {
    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";
    private static final String TEST_GRAPH_DIFFERENT_PROPERTIES = "testGraph2";

    @SuppressWarnings("unused")
    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {nodeProp1: 0.0, nodeProp2: 42})" +
        ", (b:A {nodeProp1: 1.0, nodeProp2: 43})" +
        ", (c:A {nodeProp1: 2.0, nodeProp2: 44})" +
        ", (d:B {nodeProp1: 3.0, nodeProp2: 45})" +
        ", (e:B {nodeProp1: 4.0, nodeProp2: 46})" +
        ", (f:B {nodeProp1: 5.0, nodeProp2: 47})";

    @SuppressWarnings("WeakerAccess")
    @Inject
    IdFunction idFunction;

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphProjectProc.class, GraphStreamNodePropertiesProc.class);

        runQuery(GdsCypher.call(TEST_GRAPH_SAME_PROPERTIES)
            .graphProject()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withNodeProperty("newNodeProp1", "nodeProp1")
            .withNodeProperty("newNodeProp2", "nodeProp2")
            .withAnyRelationshipType()
            .yields()
        );

        runQuery(GdsCypher.call(TEST_GRAPH_DIFFERENT_PROPERTIES)
            .graphProject()
            .withNodeLabel("A", NodeProjection.builder()
                .label("A")
                .addProperties(
                    PropertyMapping.of("newNodeProp1", "nodeProp1", 1337),
                    PropertyMapping.of("newNodeProp2", "nodeProp2", 1337)
                ).build()
            )
            .withNodeLabel("B",
                NodeProjection
                    .builder()
                    .label("B")
                    .addProperty(PropertyMapping.of("newNodeProp1", "nodeProp1", 1337))
                    .build()
            )
            .withAnyRelationshipType()
            .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
            // no labels -> defaults to PROJECT_ALL
            "",
            // explicit PROJECT_ALL
            ", ['*']"
        }
    )
    void shouldStreamNodePropertiesFromAllLabelsByDefault(String implicitOrExplicit) {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.nodeProperties.stream('%s', ['newNodeProp1', 'newNodeProp2']%s)" +
                " YIELD nodeId, nodeProperty, propertyValue, nodeLabels" +
                " RETURN nodeId AS id, nodeProperty, propertyValue, nodeLabels",
            TEST_GRAPH_SAME_PROPERTIES, implicitOrExplicit);

        assertCypherResult(graphWriteQuery, asList(
            Map.of("id", idFunction.of("a"), "nodeProperty", "newNodeProp1", "propertyValue", 0D, "nodeLabels",Collections.emptyList()),
            Map.of("id", idFunction.of("a"), "nodeProperty", "newNodeProp2", "propertyValue", 42L, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("b"), "nodeProperty", "newNodeProp1", "propertyValue", 1D, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("b"), "nodeProperty", "newNodeProp2", "propertyValue", 43L, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("c"), "nodeProperty", "newNodeProp1", "propertyValue", 2D, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("c"), "nodeProperty", "newNodeProp2", "propertyValue", 44L, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("d"), "nodeProperty", "newNodeProp1", "propertyValue", 3D, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("d"), "nodeProperty", "newNodeProp2", "propertyValue", 45L, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("e"), "nodeProperty", "newNodeProp1", "propertyValue", 4D, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("e"), "nodeProperty", "newNodeProp2", "propertyValue", 46L, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("f"), "nodeProperty", "newNodeProp1", "propertyValue", 5D, "nodeLabels", Collections.emptyList()),
            Map.of("id", idFunction.of("f"), "nodeProperty", "newNodeProp2", "propertyValue", 47L, "nodeLabels", Collections.emptyList())
        ));
    }

    @ParameterizedTest
    @ValueSource(strings = {"'A'", "['A']"})
    void shouldStreamNodePropertiesForLabelAsStringOrList(String stringOrList) {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.nodeProperties.stream('%s', ['newNodeProp1', 'newNodeProp2'], %s)" +
                " YIELD nodeId, nodeProperty, propertyValue" +
                " RETURN nodeId AS id, nodeProperty, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES, stringOrList
        );

        assertCypherResult(graphWriteQuery, asList(
            Map.of("id", idFunction.of("a"), "nodeProperty", "newNodeProp1", "propertyValue", 0D),
            Map.of("id", idFunction.of("a"), "nodeProperty", "newNodeProp2", "propertyValue", 42L),
            Map.of("id", idFunction.of("b"), "nodeProperty", "newNodeProp1", "propertyValue", 1D),
            Map.of("id", idFunction.of("b"), "nodeProperty", "newNodeProp2", "propertyValue", 43L),
            Map.of("id", idFunction.of("c"), "nodeProperty", "newNodeProp1", "propertyValue", 2D),
            Map.of("id", idFunction.of("c"), "nodeProperty", "newNodeProp2", "propertyValue", 44L)
        ));
    }

    @Test
    void shouldStreamNodePropertiesThatAreOnlyFromOneLabelEvenIfAllLabelsImplied() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.nodeProperty.stream('%s', 'newNodeProp2')" +
                " YIELD nodeId, propertyValue " +
                " RETURN nodeId AS id, propertyValue",
            TEST_GRAPH_DIFFERENT_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            Map.of("id", idFunction.of("a"), "propertyValue", 42L),
            Map.of("id", idFunction.of("b"), "propertyValue", 43L),
            Map.of("id", idFunction.of("c"), "propertyValue", 44L)
        ));
    }

    @Test
    void shouldListLabelsIfConfigIsSet() {
        assertCypherResult(
            "CALL gds.graph.nodeProperties.stream($graph, 'newNodeProp1', ['*'], {listNodeLabels: true}) " +
                " YIELD nodeId, nodeProperty, propertyValue, nodeLabels " +
                " RETURN nodeId AS id, nodeProperty, propertyValue, nodeLabels",
            Map.of("graph", TEST_GRAPH_DIFFERENT_PROPERTIES),
            asList(
                Map.of("id", idFunction.of("a"), "nodeProperty", "newNodeProp1", "propertyValue", 0D, "nodeLabels", List.of("A")),
                Map.of("id", idFunction.of("b"), "nodeProperty", "newNodeProp1", "propertyValue", 1D, "nodeLabels", List.of("A")),
                Map.of("id", idFunction.of("c"), "nodeProperty", "newNodeProp1", "propertyValue", 2D, "nodeLabels", List.of("A")),
                Map.of("id", idFunction.of("d"), "nodeProperty", "newNodeProp1", "propertyValue", 3D, "nodeLabels", List.of("B")),
                Map.of("id", idFunction.of("e"), "nodeProperty", "newNodeProp1", "propertyValue", 4D, "nodeLabels", List.of("B")),
                Map.of("id", idFunction.of("f"), "nodeProperty", "newNodeProp1", "propertyValue", 5D, "nodeLabels", List.of("B"))
            ));
    }
}
