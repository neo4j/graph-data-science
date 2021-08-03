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
import org.neo4j.gds.functions.AsNodeFunc;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.core.IdentityProperties;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.QueryExecutionException;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.ExceptionUtil.rootCause;

class GraphStreamNodePropertiesProcTest extends BaseProcTest {

    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";
    private static final String TEST_GRAPH_DIFFERENT_PROPERTIES = "testGraph2";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {id: 0, nodeProp1: 0.0, nodeProp2: 42})" +
        ", (b:A {id: 1, nodeProp1: 1.0, nodeProp2: 43})" +
        ", (c:A {id: 2, nodeProp1: 2.0, nodeProp2: 44})" +
        ", (d:B {id: 3, nodeProp1: 3.0, nodeProp2: 45})" +
        ", (e:B {id: 4, nodeProp1: 4.0, nodeProp2: 46})" +
        ", (f:B {id: 5, nodeProp1: 5.0, nodeProp2: 47})";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(GraphCreateProc.class, GraphStreamNodePropertiesProc.class);
        registerFunctions(AsNodeFunc.class);
        runQuery(DB_CYPHER);

        runQuery(GdsCypher.call()
            .withNodeLabel("A")
            .withNodeLabel("B")
            .withNodeProperty("newNodeProp1", "nodeProp1")
            .withNodeProperty("newNodeProp2", "nodeProp2")
            .withAnyRelationshipType()
            .graphCreate(TEST_GRAPH_SAME_PROPERTIES)
            .yields()
        );

        runQuery(GdsCypher.call()
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
            .graphCreate(TEST_GRAPH_DIFFERENT_PROPERTIES)
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
        "CALL gds.graph.streamNodeProperties(" +
        "   '%s', " +
        "   ['newNodeProp1', 'newNodeProp2']" +
        ") YIELD nodeId, nodeProperty, propertyValue " +
        "RETURN gds.util.asNode(nodeId).id AS id, nodeProperty, propertyValue",
        // explicit PROJECT_ALL
        "CALL gds.graph.streamNodeProperties(" +
        "   '%s', " +
        "   ['newNodeProp1', 'newNodeProp2'], " +
        "   ['*']" +
        ") YIELD nodeId, nodeProperty, propertyValue " +
        "RETURN gds.util.asNode(nodeId).id AS id, nodeProperty, propertyValue"
    })
    void streamLoadedNodeProperties(String graphWriteQueryTemplate) {
        String graphWriteQuery = formatWithLocale(graphWriteQueryTemplate, TEST_GRAPH_SAME_PROPERTIES);

        assertCypherResult(graphWriteQuery, asList(
            map("id", 0L, "nodeProperty", "newNodeProp1", "propertyValue", 0D),
            map("id", 0L, "nodeProperty", "newNodeProp2", "propertyValue", 42L),

            map("id", 1L, "nodeProperty", "newNodeProp1", "propertyValue", 1D),
            map("id", 1L, "nodeProperty", "newNodeProp2", "propertyValue", 43L),

            map("id", 2L, "nodeProperty", "newNodeProp1", "propertyValue", 2D),
            map("id", 2L, "nodeProperty", "newNodeProp2", "propertyValue", 44L),

            map("id", 3L, "nodeProperty", "newNodeProp1", "propertyValue", 3D),
            map("id", 3L, "nodeProperty", "newNodeProp2", "propertyValue", 45L),

            map("id", 4L, "nodeProperty", "newNodeProp1", "propertyValue", 4D),
            map("id", 4L, "nodeProperty", "newNodeProp2", "propertyValue", 46L),

            map("id", 5L, "nodeProperty", "newNodeProp1", "propertyValue", 5D),
            map("id", 5L, "nodeProperty", "newNodeProp2", "propertyValue", 47L)
        ));
    }

    @Test
    void streamLoadedNodePropertiesForLabel() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.streamNodeProperties(" +
            "   '%s', " +
            "   ['newNodeProp1', 'newNodeProp2'], " +
            "   ['A']" +
            ")  YIELD nodeId, nodeProperty, propertyValue " +
            "RETURN gds.util.asNode(nodeId).id AS id, nodeProperty, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", 0L, "nodeProperty", "newNodeProp1", "propertyValue", 0D),
            map("id", 0L, "nodeProperty", "newNodeProp2", "propertyValue", 42L),
            map("id", 1L, "nodeProperty", "newNodeProp1", "propertyValue", 1D),
            map("id", 1L, "nodeProperty", "newNodeProp2", "propertyValue", 43L),
            map("id", 2L, "nodeProperty", "newNodeProp1", "propertyValue", 2D),
            map("id", 2L, "nodeProperty", "newNodeProp2", "propertyValue", 44L)
        ));
    }

    @Test
    void streamLoadedNodePropertiesForLabelSubset() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.streamNodeProperties(" +
            "   '%s', " +
            "   ['newNodeProp1', 'newNodeProp2']" +
            ")  YIELD nodeId, nodeProperty, propertyValue " +
            "RETURN gds.util.asNode(nodeId).id AS id, nodeProperty, propertyValue",
            TEST_GRAPH_DIFFERENT_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", 0L, "nodeProperty", "newNodeProp1", "propertyValue", 0D),
            map("id", 0L, "nodeProperty", "newNodeProp2", "propertyValue", 42L),
            map("id", 1L, "nodeProperty", "newNodeProp1", "propertyValue", 1D),
            map("id", 1L, "nodeProperty", "newNodeProp2", "propertyValue", 43L),
            map("id", 2L, "nodeProperty", "newNodeProp1", "propertyValue", 2D),
            map("id", 2L, "nodeProperty", "newNodeProp2", "propertyValue", 44L)
        ));
    }

    @Test
    void streamMutatedNodeProperties() {
        long expectedPropertyCount = 6;

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), TEST_GRAPH_SAME_PROPERTIES).graphStore();
        NodeProperties identityProperties = new IdentityProperties(expectedPropertyCount);
        graphStore.addNodeProperty(NodeLabel.of("A"), "newNodeProp3", identityProperties);
        graphStore.addNodeProperty(NodeLabel.of("B"), "newNodeProp3", identityProperties);

        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.streamNodeProperties(" +
            "   '%s', " +
            "   ['newNodeProp3']" +
            ")  YIELD nodeId, nodeProperty, propertyValue " +
            "RETURN gds.util.asNode(nodeId).id AS id, nodeProperty, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", 0L, "nodeProperty", "newNodeProp3", "propertyValue", 0L),
            map("id", 1L, "nodeProperty", "newNodeProp3", "propertyValue", 1L),
            map("id", 2L, "nodeProperty", "newNodeProp3", "propertyValue", 2L),
            map("id", 3L, "nodeProperty", "newNodeProp3", "propertyValue", 3L),
            map("id", 4L, "nodeProperty", "newNodeProp3", "propertyValue", 4L),
            map("id", 5L, "nodeProperty", "newNodeProp3", "propertyValue", 5L)
        ));
    }

    @Test
    void shouldFailOnNonExistingNodeProperties() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.streamNodeProperties(" +
                "   '%s', " +
                "   ['newNodeProp1', 'newNodeProp2', 'newNodeProp3']" +
                ")",
                TEST_GRAPH_SAME_PROPERTIES
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(
            rootCause.getMessage(),
            containsString("No node projection with property key(s) ['newNodeProp1', 'newNodeProp2', 'newNodeProp3'] found")
        );
    }

    @Test
    void shouldFailOnNonExistingNodePropertiesForSpecificLabel() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.streamNodeProperties(" +
                "   '%s', " +
                "   ['newNodeProp1', 'newNodeProp2', 'newNodeProp3'], " +
                "   ['A'] " +
                ")",
                TEST_GRAPH_SAME_PROPERTIES
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("Node projection 'A' does not have property key 'newNodeProp3'"));
        assertThat(rootCause.getMessage(), containsString("Available keys: ['newNodeProp1', 'newNodeProp2']"));
    }

    @Test
    void streamLoadedNodePropertyForLabel() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.streamNodeProperty(" +
            "   '%s', " +
            "   'newNodeProp1', " +
            "   ['A']" +
            ")  YIELD nodeId, propertyValue " +
            "RETURN gds.util.asNode(nodeId).id AS id, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", 0L, "propertyValue", 0D),
            map("id", 1L, "propertyValue", 1D),
            map("id", 2L, "propertyValue", 2D)
        ));
    }

    @Test
    void streamLoadedNodePropertyForLabelSubset() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.streamNodeProperty(" +
            "   '%s', " +
            "   'newNodeProp2'" +
            ")  YIELD nodeId, propertyValue " +
            "RETURN gds.util.asNode(nodeId).id AS id, propertyValue",
            TEST_GRAPH_DIFFERENT_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", 0L, "propertyValue", 42L),
            map("id", 1L, "propertyValue", 43L),
            map("id", 2L, "propertyValue", 44L)
        ));
    }

    @Test
    void streamMutatedNodeProperty() {
        long expectedPropertyCount = 6;

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), TEST_GRAPH_SAME_PROPERTIES).graphStore();
        NodeProperties identityProperties = new IdentityProperties(expectedPropertyCount);
        graphStore.addNodeProperty(NodeLabel.of("A"), "newNodeProp3", identityProperties);
        graphStore.addNodeProperty(NodeLabel.of("B"), "newNodeProp3", identityProperties);

        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.streamNodeProperty(" +
            "   '%s', " +
            "   'newNodeProp3'" +
            ")  YIELD nodeId, propertyValue " +
            "RETURN gds.util.asNode(nodeId).id AS id, propertyValue",
            TEST_GRAPH_SAME_PROPERTIES
        );

        assertCypherResult(graphWriteQuery, asList(
            map("id", 0L, "propertyValue", 0L),
            map("id", 1L, "propertyValue", 1L),
            map("id", 2L, "propertyValue", 2L),
            map("id", 3L, "propertyValue", 3L),
            map("id", 4L, "propertyValue", 4L),
            map("id", 5L, "propertyValue", 5L)
        ));
    }

    @Test
    void shouldFailOnNonExistingNodeProperty() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.streamNodeProperty(" +
                "   '%s', " +
                "   'newNodeProp3'" +
                ")",
                TEST_GRAPH_SAME_PROPERTIES
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(
            rootCause.getMessage(),
            containsString("No node projection with property key(s) ['newNodeProp3'] found")
        );
    }

    @Test
    void shouldFailOnNonExistingNodePropertyForSpecificLabel() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.streamNodeProperty(" +
                "   '%s', " +
                "   'newNodeProp3', " +
                "   ['A'] " +
                ")",
                TEST_GRAPH_SAME_PROPERTIES
            ))
        );

        Throwable rootCause = rootCause(ex);
        assertEquals(IllegalArgumentException.class, rootCause.getClass());
        assertThat(rootCause.getMessage(), containsString("Node projection 'A' does not have property key 'newNodeProp3'"));
        assertThat(rootCause.getMessage(), containsString("Available keys: ['newNodeProp1', 'newNodeProp2']"));
    }
}
