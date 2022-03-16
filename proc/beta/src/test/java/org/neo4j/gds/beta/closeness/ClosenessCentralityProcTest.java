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
package org.neo4j.gds.beta.closeness;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.NonReleasingTaskRegistry;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.TestProcedureRunner;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.catalog.GraphStreamNodePropertiesProc;
import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.write.NativeNodePropertiesExporterBuilder;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.extension.Neo4jGraphExtension;
import org.neo4j.gds.transaction.TransactionContext;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Neo4jGraphExtension
class ClosenessCentralityProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (n0:Node)" +
        ", (n1:Node)" +
        ", (n2:Node)" +
        ", (n3:Node)" +
        ", (n4:Node)" +
        ", (n5:Node)" +
        ", (n6:Node)" +
        ", (n7:Node)" +
        ", (n8:Node)" +
        ", (n9:Node)" +
        ", (n10:Node)" +

        // first ring
        ", (n1)-[:TYPE]->(n2)" +
        ", (n2)-[:TYPE]->(n3)" +
        ", (n3)-[:TYPE]->(n4)" +
        ", (n4)-[:TYPE]->(n5)" +
        ", (n5)-[:TYPE]->(n1)" +

        ", (n0)-[:TYPE]->(n0)" +
        ", (n1)-[:TYPE]->(n0)" +
        ", (n2)-[:TYPE]->(n0)" +
        ", (n3)-[:TYPE]->(n0)" +
        ", (n4)-[:TYPE]->(n0)" +
        ", (n5)-[:TYPE]->(n0)" +

        // second ring
        ", (n6)-[:TYPE]->(n7)" +
        ", (n7)-[:TYPE]->(n8)" +
        ", (n8)-[:TYPE]->(n9)" +
        ", (n9)-[:TYPE]->(n10)" +
        ", (n10)-[:TYPE]->(n6)" +

        ", (n0)-[:TYPE]->(n0)" +
        ", (n0)-[:TYPE]->(n1)" +
        ", (n0)-[:TYPE]->(n2)" +
        ", (n0)-[:TYPE]->(n3)" +
        ", (n0)-[:TYPE]->(n4)" +
        ", (n0)-[:TYPE]->(n5)" +
        ", (n0)-[:TYPE]->(n6)" +
        ", (n0)-[:TYPE]->(n7)" +
        ", (n0)-[:TYPE]->(n8)" +
        ", (n0)-[:TYPE]->(n9)" +
        ", (n0)-[:TYPE]->(n10)";

    @Inject
    private IdFunction idFunction;

    private List<Map<String, Object>> expectedCentralityResult;

    @BeforeEach
    void setupGraph() throws Exception {
        expectedCentralityResult = List.of(
            Map.of("nodeId", idFunction.of("n0"), "centrality", Matchers.closeTo(1.0, 0.01)),
            Map.of("nodeId", idFunction.of("n1"), "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n2"), "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n3"), "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n4"), "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n5"), "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n6"), "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n7"), "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n8"), "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n9"), "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", idFunction.of("n10"), "centrality", Matchers.closeTo(0.588, 0.01))
        );

        registerProcedures(
            ClosenessCentralityWriteProc.class,
            ClosenessCentralityStreamProc.class,
            ClosenessCentralityMutateProc.class,
            GraphStreamNodePropertiesProc.class,
            GraphProjectProc.class
        );
    }

    @Test
    void testClosenessStream() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.closeness")
            .streamMode()
            .yields("nodeId", "centrality");

        assertCypherResult(query, expectedCentralityResult);
    }

    @Test
    void testClosenessWrite() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.closeness")
            .writeMode()
            .addParameter("writeProperty", "centrality")
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertNotEquals(-1L, row.getNumber("writeMillis"));
            assertNotEquals(-1L, row.getNumber("preProcessingMillis"));
            assertNotEquals(-1L, row.getNumber("computeMillis"));
            assertNotEquals(-1L, row.getNumber("nodes"));
            Map<String, Object> centralityDistribution = (Map<String, Object>) row.get("centralityDistribution");
            assertNotNull(centralityDistribution);
            assertEquals(1.0, (Double) centralityDistribution.get("max"), 1e-2);
        });

        assertCypherResult(
            "MATCH (n) WHERE exists(n.centrality) RETURN id(n) AS nodeId, n.centrality AS centrality",
            expectedCentralityResult
        );
    }

    @Test
    void testClosenessMutate() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);
        var query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.closeness")
            .mutateMode()
            .addParameter("mutateProperty", "centrality")
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertNotEquals(-1L, row.getNumber("mutateMillis"));
            assertNotEquals(-1L, row.getNumber("preProcessingMillis"));
            assertNotEquals(-1L, row.getNumber("computeMillis"));
            assertNotEquals(-1L, row.getNumber("nodes"));
            Map<String, Object> centralityDistribution = (Map<String, Object>) row.get("centralityDistribution");
            assertNotNull(centralityDistribution);
            assertEquals(1.0, (Double) centralityDistribution.get("max"), 1e-2);
        });

        assertCypherResult(
            "MATCH (n) WHERE exists(n.centrality) RETURN count(n) AS count ", List.of(Map.of("count", 0L)));

        assertCypherResult(
            "CALL gds.graph.streamNodeProperties('graph',['centrality']) YIELD nodeId, propertyValue AS centrality",
            expectedCentralityResult
        );
    }


    @Test
    void testProgressTracking() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME);
        TestProcedureRunner.applyOnProcedure(db, ClosenessCentralityWriteProc.class, proc -> {
            var taskStore = new GlobalTaskStore();

            proc.taskRegistryFactory = () -> new NonReleasingTaskRegistry(new TaskRegistry(getUsername(), taskStore));
            proc.nodePropertyExporterBuilder = new NativeNodePropertiesExporterBuilder(
                TransactionContext.of(proc.api, proc.procedureTransaction)
            );

            proc.write(
                DEFAULT_GRAPH_NAME,
                Map.of("writeProperty", "myProp")
            );

            assertThat(taskStore.taskStream().map(Task::description)).containsExactlyInAnyOrder(
                "ClosenessCentrality",
                "ClosenessCentrality :: WriteNodeProperties"
            );
        });
    }
}
