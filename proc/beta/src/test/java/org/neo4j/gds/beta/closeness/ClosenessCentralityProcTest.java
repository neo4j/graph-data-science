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
import org.neo4j.gds.graphbuilder.DefaultBuilder;
import org.neo4j.gds.graphbuilder.GraphBuilder;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ClosenessCentralityProcTest extends BaseProcTest {

    public static final String TYPE = "TYPE";

    private List<Map<String, Object>> expectedCentralityResult;

    @BeforeEach
    void setupGraph() throws Exception {
        DefaultBuilder builder = GraphBuilder.create(db)
                .setLabel("Node")
                .setRelationship(TYPE);

        RelationshipType type = RelationshipType.withName(TYPE);

        /*
         * create two rings of nodes where each node of ring A
         * is connected to center while center is connected to
         * each node of ring B.
         */
        Node center = builder.newDefaultBuilder()
                .setLabel("Node")
                .createNode();

        long centerNodeId = center.getId();

        builder.newRingBuilder()
            .createRing(5)
            .forEachNodeInTx(node -> node.createRelationshipTo(center, type))
            .newRingBuilder()
            .createRing(5)
            .forEachNodeInTx(node -> center.createRelationshipTo(node, type))
            .close();

        expectedCentralityResult = List.of(
            Map.of("nodeId", centerNodeId, "centrality", Matchers.closeTo(1.0, 0.01)),
            Map.of("nodeId", 1L, "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", 2L, "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", 3L, "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", 4L, "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", 5L, "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", 6L, "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", 7L, "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", 8L, "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", 9L, "centrality", Matchers.closeTo(0.588, 0.01)),
            Map.of("nodeId", 10L, "centrality", Matchers.closeTo(0.588, 0.01))
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
        String query = gdsCypher()
            .streamMode()
            .yields("nodeId", "centrality");

        assertCypherResult(query, expectedCentralityResult);
    }

    @Test
    void testClosenessWrite() {
        String query = gdsCypher()
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
        String query = gdsCypher()
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

    private GdsCypher.ModeBuildStage gdsCypher() {
        loadCompleteGraph(DEFAULT_GRAPH_NAME, Orientation.UNDIRECTED);
        return GdsCypher.call(DEFAULT_GRAPH_NAME).algo("gds.beta.closeness");
    }
}
