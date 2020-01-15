/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.centrality.ClosenessCentralityProc;
import org.neo4j.graphalgo.centrality.BetweennessCentralityProc;
import org.neo4j.graphalgo.centrality.SampledBetweennessCentralityProc;
import org.neo4j.graphalgo.spanningtree.KSpanningTreeProc;
import org.neo4j.graphalgo.spanningtree.SpanningTreeProc;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class EmptyGraphProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {

        db = TestDatabaseCreator.createTestDatabase();

        registerProcedures(
            AllShortestPathsProc.class,
            BalancedTriadsProc.class,
            BetweennessCentralityProc.class,
            ClosenessCentralityProc.class,
            KSpanningTreeProc.class,
            SpanningTreeProc.class,
            SampledBetweennessCentralityProc.class,
            ShortestPathDeltaSteppingProc.class,
            ShortestPathProc.class,
            ShortestPathsProc.class,
            SccProc.class,
            TriangleCountProc.class,
            TriangleProc.class
        );
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    private String graphImpl = "huge";

    @Test
    void testSccStream() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.scc")
            .streamMode()
            .yields();
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testSccWrite() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.scc")
            .writeMode()
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testAllShortestPathsStream() {
        boolean hasNext = runQuery("CALL algo.allShortestPaths.stream('',{graph:'" + graphImpl + "'})", Result::hasNext);
        assertFalse(hasNext);
    }

    @Test
    void testBetweennessCentralityStream() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.betweenness")
            .streamMode()
            .yields();
        boolean hasNext = runQuery(query, Result::hasNext);
        assertFalse(hasNext);
    }

    @Test
    void testBetweennessCentrality() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.betweenness")
            .writeMode()
            .yields("nodes");
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testSampledBetweennessCentralityStream() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.betweenness.sampled")
            .streamMode()
            .yields();
        boolean hasNext = runQuery(query, Result::hasNext);
        assertFalse(hasNext);
    }

    @Test
    void testSampledBetweennessCentrality() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.betweenness.sampled")
            .writeMode()
            .yields("nodes");
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testClosenessCentralityStream() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.closeness")
            .streamMode()
            .yields();
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testClosenessCentralityWrite() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.closeness")
            .writeMode()
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testTriangleCountStream() {
        String query = GdsCypher.call()
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds.alpha.triangleCount")
            .streamMode()
            .yields();
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testTriangleCountWrite() {
        String query = GdsCypher.call()
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds.alpha.triangleCount")
            .writeMode()
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("nodeCount")));
    }

    @Test
    void testTriangleStream() {
        String query = GdsCypher.call()
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds.alpha.triangle")
            .streamMode()
            .yields();
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testBalancedTriadsStream() {
        String query = GdsCypher.call()
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds.alpha.balancedTriads")
            .streamMode()
            .yields();
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testBalancedTriadsWrite() {
        String query = GdsCypher.call()
            .loadEverything(Projection.UNDIRECTED)
            .algo("gds.alpha.balancedTriads")
            .writeMode()
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("nodeCount")));
    }

    @Test
    void testKSpanningTreeKMax() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.spanningTree.kmax")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("k", 3)
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testKSpanningTreeKMin() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.spanningTree.kmin")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("k", 3)
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testSpanningTree() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.spanningTree")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("weightWriteProperty", "weight")
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testSpanningTreeMinimum() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.spanningTree.minimum")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("weightWriteProperty", "weight")
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testSpanningTreeMaximum() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.spanningTree.maximum")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("weightWriteProperty", "weight")
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testShortestPathAStarStream() {
        boolean hasNext = runQuery("CALL algo.shortestPath.astar.stream(null, null, '', '', '', {graph:'" + graphImpl + "'})", Result::hasNext);
        assertFalse(hasNext);
    }

    @Test
    void testShortestPathsStream() {
        boolean hasNext = runQuery("MATCH(n:Node {name:'s'}) WITH n CALL gds.alpha.shortestPaths.stream({startNode: n}) YIELD nodeId RETURN nodeId", Result::hasNext);
        assertFalse(hasNext);
    }

    @Test
    void testShortestPaths() {
        runQueryWithRowConsumer("MATCH(n:Node {name:'s'}) WITH n CALL gds.alpha.shortestPaths.write({startNode: n}) YIELD nodeCount RETURN nodeCount", row -> assertEquals(0L, row.getNumber("nodeCount")));
    }

    @Test
    void testShortestPathsDeltaSteppingStream() {
        boolean hasNext = runQuery("CALL algo.shortestPath.deltaStepping.stream(null, '', 0, {graph:'" + graphImpl + "'})", Result::hasNext);
        assertFalse(hasNext);
    }

    @Test
    void testShortestPathsDeltaStepping() {
        runQueryWithRowConsumer("CALL algo.shortestPath.deltaStepping(null, '', 0, {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("nodeCount")));
    }
}
