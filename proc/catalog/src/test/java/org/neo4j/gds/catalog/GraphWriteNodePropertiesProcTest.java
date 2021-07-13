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

import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.NodeProjection;
import org.neo4j.graphalgo.PropertyMapping;
import org.neo4j.graphalgo.PropertyMappings;
import org.neo4j.graphalgo.TestLog;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.core.IdentityProperties;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.degree.DegreeCentralityMutateProc;
import org.neo4j.graphalgo.pagerank.PageRankMutateProc;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.assertj.Extractors.removingThreadId;
import static org.neo4j.graphalgo.compat.GraphDatabaseApiProxy.newKernelTransaction;
import static org.neo4j.graphalgo.compat.MapUtil.map;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class GraphWriteNodePropertiesProcTest extends BaseProcTest {

    private static final String TEST_GRAPH_SAME_PROPERTIES = "testGraph";
    private static final String TEST_GRAPH_DIFFERENT_PROPERTIES = "testGraph2";

    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
        ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
        ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
        ", (d:B {nodeProp1: 3, nodeProp2: 45})" +
        ", (e:B {nodeProp1: 4, nodeProp2: 46})" +
        ", (f:B {nodeProp1: 5, nodeProp2: 47})";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            GraphCreateProc.class,
            GraphWriteNodePropertiesProc.class,
            DegreeCentralityMutateProc.class,
            PageRankMutateProc.class
        );
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
        "CALL gds.graph.writeNodeProperties(" +
        "   '%s', " +
        "   ['newNodeProp1', 'newNodeProp2']" +
        ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten",
        // explicit PROJECT_ALL
        "CALL gds.graph.writeNodeProperties(" +
        "   '%s', " +
        "   ['newNodeProp1', 'newNodeProp2'], " +
        "   ['*']" +
        ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten"
    })
    void writeLoadedNodeProperties(String graphWriteQueryTemplate) {
        String graphWriteQuery = formatWithLocale(graphWriteQueryTemplate, TEST_GRAPH_SAME_PROPERTIES);

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertThat(-1L, Matchers.lessThan(row.getNumber("writeMillis").longValue()));
            assertEquals(TEST_GRAPH_SAME_PROPERTIES, row.getString("graphName"));
            assertEquals(Arrays.asList("newNodeProp1", "newNodeProp2"), row.get("nodeProperties"));
            assertEquals(12L, row.getNumber("propertiesWritten").longValue());
        });

        String validationQuery =
            "MATCH (n) " +
            "RETURN " +
            "  n.newNodeProp1 AS newProp1, " +
            "  n.newNodeProp2 AS newProp2 " +
            "ORDER BY newProp1 ASC, newProp2 ASC";

        assertCypherResult(validationQuery, asList(
            map("newProp1", 0L, "newProp2", 42L),
            map("newProp1", 1L, "newProp2", 43L),
            map("newProp1", 2L, "newProp2", 44L),
            map("newProp1", 3L, "newProp2", 45L),
            map("newProp1", 4L, "newProp2", 46L),
            map("newProp1", 5L, "newProp2", 47L)
        ));
    }

    @Test
    void writeLoadedNodePropertiesForLabel() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.writeNodeProperties(" +
            "   '%s', " +
            "   ['newNodeProp1', 'newNodeProp2'], " +
            "   ['A']" +
            ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten",
            TEST_GRAPH_SAME_PROPERTIES
        );

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertThat(-1L, Matchers.lessThan(row.getNumber("writeMillis").longValue()));
            assertEquals(TEST_GRAPH_SAME_PROPERTIES, row.getString("graphName"));
            assertEquals(Arrays.asList("newNodeProp1", "newNodeProp2"), row.get("nodeProperties"));
            assertEquals(6L, row.getNumber("propertiesWritten").longValue());
        });

        String validationQuery =
            "MATCH (n) " +
            "RETURN " +
            "  labels(n) AS labels, " +
            "  n.newNodeProp1 AS newProp1, " +
            "  n.newNodeProp2 AS newProp2 " +
            "ORDER BY newProp1 ASC, newProp2 ASC";

        assertCypherResult(validationQuery, asList(
            map("labels", singletonList("A"), "newProp1", 0L, "newProp2", 42L),
            map("labels", singletonList("A"), "newProp1", 1L, "newProp2", 43L),
            map("labels", singletonList("A"), "newProp1", 2L, "newProp2", 44L),
            map("labels", singletonList("B"), "newProp1", null, "newProp2", null),
            map("labels", singletonList("B"), "newProp1", null, "newProp2", null),
            map("labels", singletonList("B"), "newProp1", null, "newProp2", null)
        ));
    }

    @Test
    void writeLoadedNodePropertiesForLabelSubset() {
        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.writeNodeProperties(" +
            "   '%s', " +
            "   ['newNodeProp1', 'newNodeProp2']" +
            ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten",
            TEST_GRAPH_DIFFERENT_PROPERTIES
        );

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertThat(-1L, Matchers.lessThan(row.getNumber("writeMillis").longValue()));
            assertEquals(TEST_GRAPH_DIFFERENT_PROPERTIES, row.getString("graphName"));
            assertEquals(Arrays.asList("newNodeProp1", "newNodeProp2"), row.get("nodeProperties"));
            assertEquals(6L, row.getNumber("propertiesWritten").longValue());
        });

        String validationQuery =
            "MATCH (n) " +
            "RETURN " +
            "  labels(n) AS labels, " +
            "  n.newNodeProp1 AS newProp1, " +
            "  n.newNodeProp2 AS newProp2 " +
            "ORDER BY newProp1 ASC, newProp2 ASC";

        assertCypherResult(validationQuery, asList(
            map("labels", singletonList("A"), "newProp1", 0L, "newProp2", 42L),
            map("labels", singletonList("A"), "newProp1", 1L, "newProp2", 43L),
            map("labels", singletonList("A"), "newProp1", 2L, "newProp2", 44L),
            map("labels", singletonList("B"), "newProp1", null, "newProp2", null),
            map("labels", singletonList("B"), "newProp1", null, "newProp2", null),
            map("labels", singletonList("B"), "newProp1", null, "newProp2", null)
        ));
    }

    @Test
    void writeMutatedNodeProperties() {
        long expectedPropertyCount = 6;

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), TEST_GRAPH_SAME_PROPERTIES).graphStore();
        NodeProperties identityProperties = new IdentityProperties(expectedPropertyCount);
        graphStore.addNodeProperty(NodeLabel.of("A"), "newNodeProp3", identityProperties);
        graphStore.addNodeProperty(NodeLabel.of("B"), "newNodeProp3", identityProperties);

        String graphWriteQuery = formatWithLocale(
            "CALL gds.graph.writeNodeProperties(" +
            "   '%s', " +
            "   ['newNodeProp3']" +
            ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten",
            TEST_GRAPH_SAME_PROPERTIES
        );

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertThat(-1L, Matchers.lessThan(row.getNumber("writeMillis").longValue()));
            assertEquals(TEST_GRAPH_SAME_PROPERTIES, row.getString("graphName"));
            assertEquals(singletonList("newNodeProp3"), row.get("nodeProperties"));
            assertEquals(expectedPropertyCount, row.getNumber("propertiesWritten").longValue());
        });

        String validationQuery =
            "MATCH (n) " +
            "RETURN n.newNodeProp3 AS newProp3 " +
            "ORDER BY newProp3 ASC";

        assertCypherResult(validationQuery, asList(
            map("newProp3", 0L),
            map("newProp3", 1L),
            map("newProp3", 2L),
            map("newProp3", 3L),
            map("newProp3", 4L),
            map("newProp3", 5L)
        ));
    }

    @Test
    void shouldLogProgressForIndividualLabels() {
        var log = new TestLog();

        try (var transactions = newKernelTransaction(db)) {
            var proc = new GraphWriteNodePropertiesProc();

            proc.procedureTransaction = transactions.tx();
            proc.transaction = transactions.ktx();
            proc.api = db;
            proc.callContext = ProcedureCallContext.EMPTY;
            proc.log = log;

            proc.run(TEST_GRAPH_SAME_PROPERTIES, List.of("newNodeProp1", "newNodeProp2"), List.of("*"), Map.of());
        }

        Assertions.assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "WriteNodeProperties - Label 1 of 2 [Label='A'] :: Start",
                "WriteNodeProperties - Label 1 of 2 [Label='A'] 33%",
                "WriteNodeProperties - Label 1 of 2 [Label='A'] 66%",
                "WriteNodeProperties - Label 1 of 2 [Label='A'] 100%",
                "WriteNodeProperties - Label 1 of 2 [Label='A'] :: Finished",
                "WriteNodeProperties - Label 2 of 2 [Label='B'] :: Start",
                "WriteNodeProperties - Label 2 of 2 [Label='B'] 33%",
                "WriteNodeProperties - Label 2 of 2 [Label='B'] 66%",
                "WriteNodeProperties - Label 2 of 2 [Label='B'] 100%",
                "WriteNodeProperties - Label 2 of 2 [Label='B'] :: Finished"
            );
    }

    @Test
    void shouldFailOnNonExistingNodeProperties() {
        QueryExecutionException ex = assertThrows(
            QueryExecutionException.class,
            () -> runQuery(formatWithLocale(
                "CALL gds.graph.writeNodeProperties(" +
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
                "CALL gds.graph.writeNodeProperties(" +
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
    void writePropertyTwice() {
        clearDb();
        runQuery("CREATE (:A:B)");
        runQuery("CALL gds.graph.create('myGraph', ['A','B'], '*')");

        runQuery("CALL gds.pageRank.mutate('myGraph', {nodeLabels: ['A'], mutateProperty: 'score'})");
        runQuery("CALL gds.degree.mutate('myGraph', {nodeLabels: ['B'], mutateProperty: 'score'})");

        runQuery("CALL gds.graph.writeNodeProperties('myGraph', ['score'])");

        // we write per node-label and as `B` > `A`
        // the degree-score for label `B` is written at last and overwrites the pageRank-score of label `A`
        assertCypherResult("MATCH (n) RETURN n.score AS score", List.of(Map.of("score", 0D)));
    }
}
