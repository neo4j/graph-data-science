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
package org.neo4j.gds;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.core.utils.progress.GlobalTaskStore;
import org.neo4j.gds.core.utils.progress.TaskRegistry;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.write.NativeNodePropertiesExporterBuilder;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;
import org.neo4j.gds.spanningtree.KSpanningTreeMaxProc;
import org.neo4j.gds.spanningtree.KSpanningTreeMinProc;
import org.neo4j.gds.transaction.TransactionContext;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KSpanningTreeProcTest extends BaseProcTest {

    private static String GRAPH_NAME = "graph";

    @Neo4jGraph
    static final String DB_CYPHER =
        "CREATE (a:Node {name:'a'})\n" +
        "CREATE (b:Node {name:'b'})\n" +
        "CREATE (c:Node {name:'c'})\n" +
        "CREATE (d:Node {name:'d'})\n" +

        "CREATE" +
        " (a)-[:TYPE {w:3.0}]->(b),\n" +
        " (a)-[:TYPE {w:2.0}]->(c),\n" +
        " (a)-[:TYPE {w:1.0}]->(d),\n" +
        " (b)-[:TYPE {w:1.0}]->(c),\n" +
        " (d)-[:TYPE {w:3.0}]->(c)";

    @Inject
    IdFunction idFunction;

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(KSpanningTreeMinProc.class, KSpanningTreeMaxProc.class, GraphProjectProc.class);
        var createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withRelationshipProperty("w")
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testMax() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.spanningTree.kmax")
            .writeMode()
            .addParameter("startNodeId", idFunction.of("a"))
            .addParameter("relationshipWeightProperty", "w")
            .addParameter("k", 2)
            .yields("preProcessingMillis", "computeMillis", "writeMillis");

        runQueryWithRowConsumer(query, row -> {
            assertTrue(row.getNumber("preProcessingMillis").longValue() >= 0);
            assertTrue(row.getNumber("writeMillis").longValue() >= 0);
            assertTrue(row.getNumber("computeMillis").longValue() >= 0);
        });

        final HashMap<String, Integer> communities = new HashMap<>();

        runQueryWithRowConsumer("MATCH (n) WHERE n.partition IS NOT NULL RETURN n.name as name, n.partition as p", row -> {
            final String name = row.getString("name");
            final int p = row.getNumber("p").intValue();
            communities.put(name, p);
        });

        assertEquals(communities.get("a"), communities.get("b"));
        assertEquals(communities.get("d"), communities.get("c"));
        assertNotEquals(communities.get("a"), communities.get("c"));
    }

    @Test
    void testMin() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.spanningTree.kmin")
            .writeMode()
            .addParameter("startNodeId", idFunction.of("a"))
            .addParameter("relationshipWeightProperty", "w")
            .addParameter("k", 2)
            .yields("preProcessingMillis", "computeMillis", "writeMillis");

        runQueryWithRowConsumer(query, row -> {
            assertTrue(row.getNumber("preProcessingMillis").longValue() >= 0);
            assertTrue(row.getNumber("writeMillis").longValue() >= 0);
            assertTrue(row.getNumber("computeMillis").longValue() >= 0);
        });

        final HashMap<String, Integer> communities = new HashMap<>();

        runQueryWithRowConsumer("MATCH (n) WHERE n.partition IS NOT NULL RETURN n.name as name, n.partition as p", row -> {
            final String name = row.getString("name");
            final int p = row.getNumber("p").intValue();
            communities.put(name, p);
        });

        assertEquals(communities.get("a"), communities.get("d"));
        assertEquals(communities.get("b"), communities.get("c"));
        assertNotEquals(communities.get("a"), communities.get("b"));
    }

    @Test
    void failOnInvalidStartNode() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.spanningTree.kmin")
            .writeMode()
            .addParameter("startNodeId", 42)
            .addParameter("k", 2)
            .yields();

        assertError(query, "startNode with id 42 was not loaded");
    }

    @Test
    void shouldTrackProgress() {
        TestProcedureRunner.applyOnProcedure(db, KSpanningTreeMaxProc.class, proc -> {
            var taskStore = new GlobalTaskStore();

            proc.taskRegistryFactory = jobId -> new NonReleasingTaskRegistry(new TaskRegistry(getUsername(), taskStore, jobId));
            proc.nodePropertyExporterBuilder = new NativeNodePropertiesExporterBuilder(
                TransactionContext.of(proc.databaseService, proc.procedureTransaction)
            );

            proc.kmax( // kmin or kmax doesn't matter
                GRAPH_NAME,
                Map.of(
                    "writeProperty", "myProp",
                    "k", 1L,
                    "startNodeId", 0L
                )
            );

            assertThat(taskStore.taskStream().map(Task::description)).contains(
                "KSpanningTree",
                "KSpanningTree :: WriteNodeProperties"
            );
        });
    }
}
