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
package org.neo4j.gds.paths.traverse;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

class BfsStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    private static final String DB_CYPHER =
        "CREATE" +
        "  (a:Node)" +
        ", (b:Node)" +
        ", (c:Node)" +
        ", (d:Node)" +
        ", (e:Node)" +
        ", (f:Node)" +
        ", (g:Node)" +
        ", (a)-[:TYPE]->(b)" +
        ", (a)-[:TYPE]->(c)" +
        ", (b)-[:TYPE]->(d)" +
        ", (c)-[:TYPE]->(d)" +
        ", (d)-[:TYPE]->(e)" +
        ", (d)-[:TYPE]->(f)" +
        ", (e)-[:TYPE]->(g)" +
        ", (f)-[:TYPE]->(g)";

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(BfsStatsProc.class, GraphProjectProc.class);
    }

    @Test
    void testCompute() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE")
            .yields();
        runQuery(createQuery);

        long id = idFunction.of("a");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("bfs")
            .statsMode()
            .addParameter("sourceNode", id)
            .addParameter("targetNodes", List.of(idFunction.of("e"), idFunction.of("f")))
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "configuration", isA(Map.class)
        )));
    }

    @Test
    void testEstimate() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withRelationshipType("TYPE")
            .yields();
        runQuery(createQuery);

        long id = idFunction.of("a");
        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("bfs")
            .statsEstimation()
            .addParameter("sourceNode", id)
            .addParameter("targetNodes", List.of(idFunction.of("e"), idFunction.of("f")))
            .yields("bytesMin", "bytesMax", "nodeCount", "relationshipCount");

        assertCypherResult(query, List.of(Map.of(
            "bytesMin", greaterThan(0L),
            "bytesMax", greaterThan(0L),
            "nodeCount", 7L,
            "relationshipCount", 8L
        )));
    }
}
