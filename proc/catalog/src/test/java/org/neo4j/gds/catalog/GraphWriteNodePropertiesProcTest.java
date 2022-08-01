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
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.NodeProjection;
import org.neo4j.gds.PropertyMapping;
import org.neo4j.gds.PropertyMappings;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.IdentityPropertyValues;
import org.neo4j.gds.core.utils.warnings.GlobalUserLogStore;
import org.neo4j.gds.core.utils.warnings.UserLogRegistryExtension;
import org.neo4j.gds.core.write.NativeNodePropertiesExporterBuilder;
import org.neo4j.gds.degree.DegreeCentralityMutateProc;
import org.neo4j.gds.pagerank.PageRankMutateProc;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.TestProcedureRunner.applyOnProcedure;
import static org.neo4j.gds.assertj.Extractors.removingThreadId;
import static org.neo4j.gds.compat.MapUtil.map;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

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
        registerProcedures(
            GraphProjectProc.class,
            GraphWriteNodePropertiesProc.class,
            DegreeCentralityMutateProc.class,
            PageRankMutateProc.class
        );
        runQuery(DB_CYPHER);

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
        "CALL gds.graph.nodeProperties.write(" +
        "   '%s', " +
        "   ['newNodeProp1', 'newNodeProp2']" +
        ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten",
        // explicit PROJECT_ALL
        "CALL gds.graph.nodeProperties.write(" +
        "   '%s', " +
        "   ['newNodeProp1', 'newNodeProp2'], " +
        "   ['*']" +
        ") YIELD writeMillis, graphName, nodeProperties, propertiesWritten"
    })
    void writeLoadedNodeProperties(String graphWriteQueryTemplate) {
        String graphWriteQuery = formatWithLocale(graphWriteQueryTemplate, TEST_GRAPH_SAME_PROPERTIES);

        runQueryWithRowConsumer(graphWriteQuery, row -> {
            assertThat(row.getNumber("writeMillis").longValue()).isGreaterThan(-1L);
            assertThat(row.getString("graphName")).isEqualTo(TEST_GRAPH_SAME_PROPERTIES);
            assertThat(row.get("nodeProperties")).isEqualTo(Arrays.asList("newNodeProp1", "newNodeProp2"));
            assertThat(row.getNumber("propertiesWritten").longValue()).isEqualTo(12L);
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
        assertCypherResult(
            "CALL gds.graph.nodeProperties.write($graph, ['newNodeProp1', 'newNodeProp2'], ['A'])",
            Map.of("graph", TEST_GRAPH_SAME_PROPERTIES),
            List.of(Map.of(
                "writeMillis", Matchers.greaterThan(-1L),
                "graphName", TEST_GRAPH_SAME_PROPERTIES,
                "nodeProperties", Arrays.asList("newNodeProp1", "newNodeProp2"),
                "propertiesWritten", 6L
            ))
        );

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
        assertCypherResult(
            "CALL gds.graph.nodeProperties.write($graph, ['newNodeProp1', 'newNodeProp2'])",
            Map.of("graph", TEST_GRAPH_DIFFERENT_PROPERTIES),
            List.of(Map.of(
                "writeMillis", Matchers.greaterThan(-1L),
                "graphName", TEST_GRAPH_DIFFERENT_PROPERTIES,
                "nodeProperties", Arrays.asList("newNodeProp1", "newNodeProp2"),
                "propertiesWritten", 6L
            ))
        );

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

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), DatabaseId.of(db), TEST_GRAPH_SAME_PROPERTIES).graphStore();
        NodePropertyValues identityProperties = new IdentityPropertyValues(expectedPropertyCount);
        graphStore.addNodeProperty(Set.of(NodeLabel.of("A"), NodeLabel.of("B")), "newNodeProp3", identityProperties);

        assertCypherResult(
            "CALL gds.graph.nodeProperties.write($graph, ['newNodeProp3'])",
            Map.of("graph", TEST_GRAPH_SAME_PROPERTIES),
            List.of(Map.of(
                "writeMillis", Matchers.greaterThan(-1L),
                "graphName", TEST_GRAPH_SAME_PROPERTIES,
                "nodeProperties", List.of("newNodeProp3"),
                "propertiesWritten", expectedPropertyCount
            ))
        );

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
        var log = Neo4jProxy.testLog();

        applyOnProcedure(
            db,
            GraphWriteNodePropertiesProc.class,
            log,
            proc -> {
                proc.nodePropertyExporterBuilder = new NativeNodePropertiesExporterBuilder(TransactionContext.of(proc.api, proc.procedureTransaction));
                proc.run(TEST_GRAPH_SAME_PROPERTIES, List.of("newNodeProp1", "newNodeProp2"), List.of("*"), Map.of());
            }
        );

        assertThat(log.getMessages(TestLog.INFO))
            .extracting(removingThreadId())
            .contains(
                "WriteNodeProperties :: Label 1 of 2 :: Start",
                "WriteNodeProperties :: Label 1 of 2 33%",
                "WriteNodeProperties :: Label 1 of 2 66%",
                "WriteNodeProperties :: Label 1 of 2 100%",
                "WriteNodeProperties :: Label 1 of 2 :: Finished",
                "WriteNodeProperties :: Label 2 of 2 :: Start",
                "WriteNodeProperties :: Label 2 of 2 33%",
                "WriteNodeProperties :: Label 2 of 2 66%",
                "WriteNodeProperties :: Label 2 of 2 100%",
                "WriteNodeProperties :: Label 2 of 2 :: Finished"
            );
    }

    @Test
    void shouldFailOnNonExistingNodeProperties() {
        assertError(
            "CALL gds.graph.nodeProperties.write($graph, ['newNodeProp1', 'newNodeProp2', 'newNodeProp3'])",
            Map.of("graph", TEST_GRAPH_SAME_PROPERTIES),
            "Expecting at least one node projection to contain property key(s) ['newNodeProp1', 'newNodeProp2', 'newNodeProp3']."
        );
    }

    @Test
    void shouldFailOnNonExistingNodePropertiesForSpecificLabel() {
        assertError(
            "CALL gds.graph.nodeProperties.write(" +
            "   $graph, " +
            "   ['newNodeProp1', 'newNodeProp2', 'newNodeProp3'], " +
            "   ['A'] " +
            ")",
            Map.of("graph", TEST_GRAPH_SAME_PROPERTIES),
            "Expecting all specified node projections to have all given properties defined. " +
            "Could not find property key(s) ['newNodeProp3'] for label A. " +
            "Defined keys: ['newNodeProp1', 'newNodeProp2']."
        );
    }

    @Test
    void shouldLogDeprecationWarning() {
        runQuery("CALL gds.graph.writeNodeProperties($graph, ['newNodeProp1', 'newNodeProp2'])", Map.of("graph", TEST_GRAPH_SAME_PROPERTIES));
        var userLogEntries = userLogStore.query(getUsername()).collect(Collectors.toList());
        Assertions.assertThat(userLogEntries.size()).isEqualTo(1);
        Assertions.assertThat(userLogEntries.get(0).getMessage())
            .contains("deprecated")
            .contains("gds.graph.nodeProperties.write");
    }
}
