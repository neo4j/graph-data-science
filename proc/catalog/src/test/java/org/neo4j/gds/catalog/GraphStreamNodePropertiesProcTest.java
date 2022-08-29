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
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.IdentityPropertyValues;
import org.neo4j.gds.core.utils.warnings.GlobalUserLogStore;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryExtension;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

class GraphStreamNodePropertiesProcTest extends BaseProcTest {

    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";
    private static final String TEST_GRAPH_DIFFERENT_PROPERTIES = "testGraph2";

    @Neo4jGraph(offsetIds = true)
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {nodeProp1: 0.0, nodeProp2: 42})" +
        ", (b:A {nodeProp1: 1.0, nodeProp2: 43})" +
        ", (c:A {nodeProp1: 2.0, nodeProp2: 44})" +
        ", (d:B {nodeProp1: 3.0, nodeProp2: 45})" +
        ", (e:B {nodeProp1: 4.0, nodeProp2: 46})" +
        ", (f:B {nodeProp1: 5.0, nodeProp2: 47})";

    @Inject
    IdFunction idFunction;

    GlobalUserLogStore userLogStore;

    @Override
    @ExtensionCallback
    protected void configuration(TestDatabaseManagementServiceBuilder builder) {
        super.configuration(builder);
        this.userLogStore = new GlobalUserLogStore();
        builder.removeExtensions(extension -> extension instanceof UserLogRegistryExtension);
        builder.addExtension(new UserLogRegistryExtension(() -> userLogStore));
    }

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
            .withNodeLabel("A", NodeProjection.of(
                "A",
                PropertyMappings.of().withMappings(
                    PropertyMapping.of("newNodeProp1", "nodeProp1", 1337),
                    PropertyMapping.of("newNodeProp2", "nodeProp2", 1337)
                )
            ))
            .withNodeLabel("B", NodeProjection.of(
                "B",
                PropertyMappings.of().withMappings(
                    PropertyMapping.of("newNodeProp1", "nodeProp1", 1337)
                )
            ))
            .withAnyRelationshipType()
            .yields()
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest
    @ValueSource(strings = {
        // no labels -> defaults to PROJECT_ALL
        "CALL gds.graph.nodeProperties.stream(" +
        "   '%s', " +
        "   ['newNodeProp1', 'newNodeProp2']" +
        ") YIELD nodeId, nodeProperty, propertyValue " +
        "RETURN nodeId AS id, nodeProperty, propertyValue",
        // explicit PROJECT_ALL
        "CALL gds.graph.nodeProperties.stream(" +
        "   '%s', " +
        "   ['newNodeProp1', 'newNodeProp2'], " +
        "   ['*']" +
        ") YIELD nodeId, nodeProperty, propertyValue " +
        "RETURN nodeId AS id, nodeProperty, propertyValue"
    })
    void streamLoadedNodeProperties(String graphWriteQueryTemplate) {
        String graphWriteQuery = formatWithLocale(graphWriteQueryTemplate, TEST_GRAPH_SAME_PROPERTIES);

        assertCypherResult(graphWriteQuery, asList(
            map("id", idFunction.of("a"), "nodeProperty", "newNodeProp1", "propertyValue", 0D),
            map("id", idFunction.of("a"), "nodeProperty", "newNodeProp2", "propertyValue", 42L),
            map("id", idFunction.of("b"), "nodeProperty", "newNodeProp1", "propertyValue", 1D),
            map("id", idFunction.of("b"), "nodeProperty", "newNodeProp2", "propertyValue", 43L),
            map("id", idFunction.of("c"), "nodeProperty", "newNodeProp1", "propertyValue", 2D),
            map("id", idFunction.of("c"), "nodeProperty", "newNodeProp2", "propertyValue", 44L),
            map("id", idFunction.of("d"), "nodeProperty", "newNodeProp1", "propertyValue", 3D),
            map("id", idFunction.of("d"), "nodeProperty", "newNodeProp2", "propertyValue", 45L),
            map("id", idFunction.of("e"), "nodeProperty", "newNodeProp1", "propertyValue", 4D),
            map("id", idFunction.of("e"), "nodeProperty", "newNodeProp2", "propertyValue", 46L),
            map("id", idFunction.of("f"), "nodeProperty", "newNodeProp1", "propertyValue", 5D),
            map("id", idFunction.of("f"), "nodeProperty", "newNodeProp2", "propertyValue", 47L)
        ));
    }

    @Test
    void streamLoadedNodePropertiesForLabel() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.nodeProperties.stream('%s', ['newNodeProp1', 'newNodeProp2'], ['A'])" +
            " YIELD nodeId, nodeProperty, propertyValue" +
            " RETURN nodeId AS id, nodeProperty, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", idFunction.of("a"), "nodeProperty", "newNodeProp1", "propertyValue", 0D),
            map("id", idFunction.of("a"), "nodeProperty", "newNodeProp2", "propertyValue", 42L),
            map("id", idFunction.of("b"), "nodeProperty", "newNodeProp1", "propertyValue", 1D),
            map("id", idFunction.of("b"), "nodeProperty", "newNodeProp2", "propertyValue", 43L),
            map("id", idFunction.of("c"), "nodeProperty", "newNodeProp1", "propertyValue", 2D),
            map("id", idFunction.of("c"), "nodeProperty", "newNodeProp2", "propertyValue", 44L)
        ));
    }

    @Test
    void streamLoadedNodePropertiesForLabelSubset() {
        assertCypherResult(
            "CALL gds.graph.nodeProperties.stream($graph, ['newNodeProp1', 'newNodeProp2']) " +
            " YIELD nodeId, nodeProperty, propertyValue " +
            " RETURN nodeId AS id, nodeProperty, propertyValue",
            Map.of("graph", TEST_GRAPH_DIFFERENT_PROPERTIES),
            asList(
                map("id", idFunction.of("a"), "nodeProperty", "newNodeProp1", "propertyValue", 0D),
                map("id", idFunction.of("a"), "nodeProperty", "newNodeProp2", "propertyValue", 42L),
                map("id", idFunction.of("b"), "nodeProperty", "newNodeProp1", "propertyValue", 1D),
                map("id", idFunction.of("b"), "nodeProperty", "newNodeProp2", "propertyValue", 43L),
                map("id", idFunction.of("c"), "nodeProperty", "newNodeProp1", "propertyValue", 2D),
                map("id", idFunction.of("c"), "nodeProperty", "newNodeProp2", "propertyValue", 44L)
            ));
    }

    @Test
    void streamMutatedNodeProperties() {
        long expectedPropertyCount = 6;

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), TEST_GRAPH_SAME_PROPERTIES).graphStore();
        NodePropertyValues identityProperties = new IdentityPropertyValues(expectedPropertyCount);
        graphStore.addNodeProperty(Set.of(NodeLabel.of("A"), NodeLabel.of("B")), "newNodeProp3", identityProperties);

        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.nodeProperties.stream(" +
            "   '%s', " +
            "   ['newNodeProp3']" +
            ")  YIELD nodeId, nodeProperty, propertyValue " +
            "RETURN nodeId AS id, nodeProperty, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", idFunction.of("a"), "nodeProperty", "newNodeProp3", "propertyValue", 0L),
            map("id", idFunction.of("b"), "nodeProperty", "newNodeProp3", "propertyValue", 1L),
            map("id", idFunction.of("c"), "nodeProperty", "newNodeProp3", "propertyValue", 2L),
            map("id", idFunction.of("d"), "nodeProperty", "newNodeProp3", "propertyValue", 3L),
            map("id", idFunction.of("e"), "nodeProperty", "newNodeProp3", "propertyValue", 4L),
            map("id", idFunction.of("f"), "nodeProperty", "newNodeProp3", "propertyValue", 5L)
        ));
    }

    @Test
    void shouldFailOnNonExistingNodeProperties() {
        assertError(
            "CALL gds.graph.nodeProperties.stream($graph, ['newNodeProp1', 'newNodeProp2', 'newNodeProp3'])",
            Map.of("graph", TEST_GRAPH_SAME_PROPERTIES),
            "Expecting at least one node projection to contain property key(s) ['newNodeProp1', 'newNodeProp2', 'newNodeProp3']."
        );
    }

    @Test
    void shouldFailOnNonExistingNodePropertiesForSpecificLabel() {
        assertError(
            "CALL gds.graph.nodeProperties.stream($graph, ['newNodeProp1', 'newNodeProp2', 'newNodeProp3'], ['A'])",
            Map.of("graph", TEST_GRAPH_SAME_PROPERTIES),
            "Expecting all specified node projections to have all given properties defined. " +
            "Could not find property key(s) ['newNodeProp3'] for label A. " +
            "Defined keys: ['newNodeProp1', 'newNodeProp2']."
        );
    }

    @Test
    void streamLoadedNodePropertyForLabel() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.nodeProperty.stream(" +
            "   '%s', " +
            "   'newNodeProp1', " +
            "   ['A']" +
            ")  YIELD nodeId, propertyValue " +
            "RETURN nodeId AS id, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", idFunction.of("a"), "propertyValue", 0D),
            map("id", idFunction.of("b"), "propertyValue", 1D),
            map("id", idFunction.of("c"), "propertyValue", 2D)
        ));
    }

    @Test
    void streamLoadedNodePropertyForLabelSubset() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.nodeProperty.stream(" +
            "   '%s', " +
            "   'newNodeProp2'" +
            ")  YIELD nodeId, propertyValue " +
            "RETURN nodeId AS id, propertyValue",
            TEST_GRAPH_DIFFERENT_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", idFunction.of("a"), "propertyValue", 42L),
            map("id", idFunction.of("b"), "propertyValue", 43L),
            map("id", idFunction.of("c"), "propertyValue", 44L)
        ));
    }

    @Test
    void streamMutatedNodeProperty() {
        long expectedPropertyCount = 6;

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), TEST_GRAPH_SAME_PROPERTIES).graphStore();
        NodePropertyValues identityProperties = new IdentityPropertyValues(expectedPropertyCount);
        graphStore.addNodeProperty(Set.of(NodeLabel.of("A"), NodeLabel.of("B")), "newNodeProp3", identityProperties);

        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.nodeProperty.stream(" +
            "   '%s', " +
            "   'newNodeProp3'" +
            ")  YIELD nodeId, propertyValue " +
            "RETURN nodeId AS id, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", idFunction.of("a"), "propertyValue", 0L),
            map("id", idFunction.of("b"), "propertyValue", 1L),
            map("id", idFunction.of("c"), "propertyValue", 2L),
            map("id", idFunction.of("d"), "propertyValue", 3L),
            map("id", idFunction.of("e"), "propertyValue", 4L),
            map("id", idFunction.of("f"), "propertyValue", 5L)
        ));
    }

    @Test
    void shouldFailOnNonExistingNodeProperty() {
        assertError(
            "CALL gds.graph.nodeProperty.stream($graph, 'newNodeProp3')",
            Map.of("graph", TEST_GRAPH_SAME_PROPERTIES),
            "Expecting at least one node projection to contain property key(s) ['newNodeProp3']."
        );
    }

    @Test
    void shouldFailOnNonExistingNodePropertyForSpecificLabel() {
        assertError(
            "CALL gds.graph.nodeProperty.stream($graph, 'newNodeProp3', ['A'])",
            Map.of("graph", TEST_GRAPH_SAME_PROPERTIES),
            "Expecting all specified node projections to have all given properties defined. " +
            "Could not find property key(s) ['newNodeProp3'] for label A. " +
            "Defined keys: ['newNodeProp1', 'newNodeProp2']"
        );
    }

    static Stream<Arguments> proceduresAndArguments() {
        return Stream.of(
            Arguments.of("gds.graph.streamNodeProperty", "gds.graph.nodeProperty.stream", "'newNodeProp1'"),
            Arguments.of("gds.graph.streamNodeProperties", "gds.graph.nodeProperties.stream", "['newNodeProp1', 'newNodeProp2']")
        );
    }

    @ParameterizedTest
    @MethodSource("proceduresAndArguments")
    void shouldLogDeprecationWarning(String deprecatedProcedure, String newProcedure, String properties) {
        runQuery(formatWithLocale("CALL %s($graph, %s)", deprecatedProcedure, properties), Map.of("graph", TEST_GRAPH_SAME_PROPERTIES));
        var userLogEntries = userLogStore.query(getUsername()).collect(Collectors.toList());
        assertThat(userLogEntries.size()).isEqualTo(1);
        assertThat(userLogEntries.get(0).getMessage())
            .contains("deprecated")
            .contains(newProcedure);
    }
}
