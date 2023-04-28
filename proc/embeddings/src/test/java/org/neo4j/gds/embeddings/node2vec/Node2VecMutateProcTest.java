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
package org.neo4j.gds.embeddings.node2vec;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphWriteNodePropertiesProc;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.graphdb.QueryExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class Node2VecMutateProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node1 {dummy:1})" +
        ", (b:Node1 {dummy:1})" +
        ", (c:Node2 {dummy:1})" +
        ", (d:Isolated {dummy:1})" +
        ", (e:Isolated {dummy:1})" +
        ", (a)-[:REL]->(b)" +
        ", (b)-[:REL]->(a)" +
        ", (a)-[:REL]->(c)" +
        ", (c)-[:REL]->(a)" +
        ", (b)-[:REL]->(c)" +
        ", (c)-[:REL]->(b)";

    @BeforeEach
    void loadProcedures() throws Exception {
        registerProcedures(
            Node2VecMutateProc.class,
            GraphWriteNodePropertiesProc.class,
            GraphProjectProc.class
        );

        runQuery("CALL gds.graph.project('graph', '*', '*')");
    }

    @AfterEach
    void removeAllLoadedGraphs() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @Test
    void testMutateFailsOnExistingToken() {

        var projectQuery = "CALL gds.graph.project('existingPropertyGraph', {N: {label: 'Node1', properties: 'dummy'}}, '*')";
        runQuery(projectQuery);

        var query = GdsCypher.call("existingPropertyGraph")
            .algo("gds.beta.node2vec")
            .mutateMode()
            .addParameter("embeddingDimension", 42)
            .addParameter("mutateProperty", "dummy")
            .addParameter("iterations", 1)
            .yields();

        assertThatExceptionOfType(QueryExecutionException.class)
            .isThrownBy(() -> runQuery(query))
            .withRootCauseExactlyInstanceOf(IllegalArgumentException.class)
            .withMessageContaining("Node property `dummy` already exists in the in-memory graph.");
    }

    @Test
    void mutation() {
        var graphBeforeMutation = findLoadedGraph("graph");
        assertThat(graphBeforeMutation.schema().nodeSchema().allProperties()).doesNotContain("testProp");

        var rowCount = runQueryWithRowConsumer(
            "CALL gds.beta.node2vec.mutate('graph', {" +
            "   embeddingDimension: 42, " +
            "   mutateProperty: 'testProp', " +
            "   iterations: 5" +
            "}) " +
            " YIELD lossPerIteration",
            row -> {
                assertThat(row.get("lossPerIteration"))
                    .as("There should be the same amount of losses as the configured `iterations`.")
                    .asList()
                    .hasSize(5);
            }
        );

        assertThat(rowCount)
            .as("`mutate` mode should always return one row")
            .isEqualTo(1);

        var graphAfterMutation = findLoadedGraph("graph");
        assertThat(graphAfterMutation.schema().nodeSchema().allProperties()).contains("testProp");

        var mutatedProperty = graphAfterMutation.nodeProperties("testProp");
        graphAfterMutation.forEachNode(nodeId -> {
            assertThat(mutatedProperty.floatArrayValue(nodeId)).hasSize(42);
            return true;
        });
    }
}
