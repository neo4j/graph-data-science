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
import org.neo4j.graphalgo.unionfind.MSColoringProc;
import org.neo4j.graphdb.Result;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class EmptyGraphProcTest extends ProcTestBase {

    @BeforeEach
    void setup() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        registerProcedures(
            AllShortestPathsProc.class,
            BetweennessCentralityProc.class,
            ClosenessCentralityProc.class,
            DangalchevCentralityProc.class,
            HarmonicCentralityProc.class,
            KShortestPathsProc.class,
            KSpanningTreeProc.class,
            MSColoringProc.class,
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
    void testUnionFindMSColoringStream() {
        Result result = db.execute("CALL algo.unionFind.mscoloring.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testUnionFindMSColoring() throws Exception {
        db.execute("CALL algo.unionFind.mscoloring('', '',{graph:'" + graphImpl + "'}) YIELD nodes")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    void testStronglyConnectedComponentsStream() {
        Result result = db.execute("CALL algo.scc.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testStronglyConnectedComponents() throws Exception {
        db.execute("CALL algo.scc('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    void testStronglyConnectedComponentsMultiStepStream() {
        Result result = db.execute("CALL algo.scc.stream('', '',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testStronglyConnectedComponentsMultiStep() throws Exception {
        db.execute("CALL algo.scc('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    void testStronglyConnectedComponentsTunedTarjan() throws Exception {
        db.execute("CALL algo.scc.recursive.tunedTarjan('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("setCount"));
                    return true;
                });
    }

    @Test
    void testStronglyConnectedComponentsTunedTarjanStream() {
        Result result = db.execute("CALL algo.scc.recursive.tunedTarjan.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testForwardBackwardStronglyConnectedComponentsStream() {
        Result result = db.execute("CALL algo.scc.forwardBackward.stream(0, '', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testAllShortestPathsStream() {
        Result result = db.execute("CALL algo.allShortestPaths.stream('',{graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testBetweennessCentralityStream() {
        Result result = db.execute("CALL algo.betweenness.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testBetweennessCentrality() throws Exception {
        db.execute("CALL algo.betweenness('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    void testSampledBetweennessCentralityStream() {
        Result result = db.execute("CALL algo.betweenness.sampled.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testSampledBetweennessCentrality() throws Exception {
        db.execute("CALL algo.betweenness.sampled('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    void testClosenessCentralityStream() {
        Result result = db.execute("CALL algo.closeness.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testClosenessCentrality() throws Exception {
        db.execute("CALL algo.closeness('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    void testTriangleCountStream() {
        Result result = db.execute("CALL algo.triangleCount.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testTriangleCount() throws Exception {
        db.execute("CALL algo.triangleCount('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodeCount"));
                    return true;
                });
    }

    @Test
    void testTriangleStream() {
        Result result = db.execute("CALL algo.triangle.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testDangelchevCentralityStream() {
        Result result = db.execute("CALL algo.closeness.dangalchev.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testDangelchevCentrality() throws Exception {
        db.execute("CALL algo.closeness.dangalchev('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    void testHarmonicCentralityStream() {
        Result result = db.execute("CALL algo.closeness.harmonic.stream('', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testHarmonicCentrality() throws Exception {
        db.execute("CALL algo.closeness.harmonic('', '',{graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodes"));
                    return true;
                });
    }

    @Test
    void testKSpanningTreeKMax() throws Exception {
        db.execute("CALL algo.spanningTree.kmax('', '', '', 0, 3, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    void testKSpanningTreeKMin() throws Exception {
        db.execute("CALL algo.spanningTree.kmin('', '', '', 0, 3, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    void testMST() throws Exception {
        db.execute("CALL algo.mst('', '', '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    void testSpanningTree() throws Exception {
        db.execute("CALL algo.spanningTree('', '', '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    void testSpanningTreeMinimum() throws Exception {
        db.execute("CALL algo.spanningTree.minimum('', '', '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    void testSpanningTreeMaximum() throws Exception {
        db.execute("CALL algo.spanningTree.maximum('', '', '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("effectiveNodeCount"));
                    return true;
                });
    }

    @Test
    void testShortestPathAStarStream() {
        Result result = db.execute("CALL algo.shortestPath.astar.stream(null, null, '', '', '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testShortestPathStream() {
        Result result = db.execute("CALL algo.shortestPath.stream(null, null, '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testShortestPath() throws Exception {
        db.execute("CALL algo.shortestPath(null, null, '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodeCount"));
                    return true;
                });
    }

    @Test
    void testShortestPathsStream() {
        Result result = db.execute("CALL algo.shortestPaths.stream(null, '', {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testShortestPaths() throws Exception {
        db.execute("CALL algo.shortestPaths(null, '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodeCount"));
                    return true;
                });
    }

    @Test
    void testKShortestPaths() throws Exception {
        db.execute("CALL algo.kShortestPaths(null, null, 3, '', {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("resultCount"));
                    return true;
                });
    }

    @Test
    void testShortestPathsDeltaSteppingStream() {
        Result result = db.execute("CALL algo.shortestPath.deltaStepping.stream(null, '', 0, {graph:'" + graphImpl + "'})");
        assertFalse(result.hasNext());
    }

    @Test
    void testShortestPathsDeltaStepping() throws Exception {
        db.execute("CALL algo.shortestPath.deltaStepping(null, '', 0, {graph:'" + graphImpl + "'})")
                .accept((Result.ResultVisitor<Exception>) row -> {
                    assertEquals(0L, row.getNumber("nodeCount"));
                    return true;
                });
    }
}
