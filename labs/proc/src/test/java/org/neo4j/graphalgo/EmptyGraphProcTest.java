/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class EmptyGraphProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {

        db = TestDatabaseCreator.createTestDatabase();

        registerProcedures(
            AllShortestPathsProc.class,
            BetweennessCentralityProc.class,
            ClosenessCentralityProc.class,
            KShortestPathsProc.class,
            KSpanningTreeProc.class,
            PrimProc.class,
            ShortestPathDeltaSteppingProc.class,
            ShortestPathProc.class,
            ShortestPathsProc.class,
            StronglyConnectedComponentsProc.class,
            TriangleProc.class
        );
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    private String graphImpl = "huge";

    @Test
    void testStronglyConnectedComponentsStream() {
        Result result = runQuery("CALL algo.scc.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testStronglyConnectedComponents() {
        runQuery("CALL algo.scc('', '',{graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("setCount")));
    }

    @Test
    void testAllShortestPathsStream() {
        Result result = runQuery("CALL algo.allShortestPaths.stream('',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testBetweennessCentralityStream() {
        Result result = runQuery("CALL algo.betweenness.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testBetweennessCentrality() {
        runQuery("CALL algo.betweenness('', '',{graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testSampledBetweennessCentralityStream() {
        Result result = runQuery("CALL algo.betweenness.sampled.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testSampledBetweennessCentrality() {
        runQuery("CALL algo.betweenness.sampled('', '',{graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testClosenessCentralityStream() {
        Result result = runQuery("CALL algo.closeness.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testClosenessCentrality() {
        runQuery("CALL algo.closeness('', '',{graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testTriangleCountStream() {
        Result result = runQuery("CALL algo.triangleCount.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testTriangleCount() {
        runQuery("CALL algo.triangleCount('', '',{graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("nodeCount")));
    }

    @Test
    void testTriangleStream() {
        Result result = runQuery("CALL algo.triangle.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testKSpanningTreeKMax() {
        runQuery("CALL algo.spanningTree.kmax('', '', '', 0, 3, {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testKSpanningTreeKMin() {
        runQuery("CALL algo.spanningTree.kmin('', '', '', 0, 3, {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testMST() {
        runQuery("CALL algo.mst('', '', '', 0, {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testSpanningTree() {
        runQuery("CALL algo.spanningTree('', '', '', 0, {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testSpanningTreeMinimum() {
        runQuery("CALL algo.spanningTree.minimum('', '', '', 0, {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testSpanningTreeMaximum() {
        runQuery("CALL algo.spanningTree.maximum('', '', '', 0, {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testShortestPathAStarStream() {
        Result result = runQuery("CALL algo.shortestPath.astar.stream(null, null, '', '', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testShortestPathStream() {
        Result result = runQuery("CALL algo.shortestPath.stream(null, null, '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testShortestPath() {
        runQuery("CALL algo.shortestPath(null, null, '', {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("nodeCount")));
    }

    @Test
    void testShortestPathsStream() {
        Result result = runQuery("CALL algo.shortestPaths.stream(null, '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testShortestPaths() {
        runQuery("CALL algo.shortestPaths(null, '', {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("nodeCount")));
    }

    @Test
    void testKShortestPaths() {
        runQuery("CALL algo.kShortestPaths(null, null, 3, '', {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("resultCount")));
    }

    @Test
    void testShortestPathsDeltaSteppingStream() {
        Result result = runQuery("CALL algo.shortestPath.deltaStepping.stream(null, '', 0, {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testShortestPathsDeltaStepping() {
        runQuery("CALL algo.shortestPath.deltaStepping(null, '', 0, {graph:'" + graphImpl + "'})", row -> assertEquals(0L, row.getNumber("nodeCount")));
    }
}
