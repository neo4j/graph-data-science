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
import org.neo4j.gds.centrality.ClosenessCentralityProc;
import org.neo4j.gds.centrality.HarmonicCentralityProc;
import org.neo4j.gds.influenceΜaximization.CELFProc;
import org.neo4j.gds.influenceΜaximization.GreedyProc;
import org.neo4j.gds.scc.SccProc;
import org.neo4j.gds.shortestpath.ShortestPathDeltaSteppingProc;
import org.neo4j.gds.shortestpaths.AllShortestPathsProc;
import org.neo4j.gds.spanningtree.KSpanningTreeProc;
import org.neo4j.gds.spanningtree.SpanningTreeProc;
import org.neo4j.gds.triangle.TriangleProc;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class EmptyGraphProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            AllShortestPathsProc.class,
            ClosenessCentralityProc.class,
            HarmonicCentralityProc.class,
            KSpanningTreeProc.class,
            SpanningTreeProc.class,
            ShortestPathDeltaSteppingProc.class,
            SccProc.class,
            TriangleProc.class,
            GreedyProc.class,
            CELFProc.class
        );
    }

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
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.allShortestPaths")
            .streamMode()
            .yields();
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
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
    void testHarmonicCentralityStream() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.closeness.harmonic")
            .streamMode()
            .yields();
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testHarmonicCentralityWrite() {
        String query = GdsCypher.call()
            .withAnyLabel()
            .withAnyRelationshipType()
            .algo("gds.alpha.closeness.harmonic")
            .writeMode()
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testTriangleStream() {
        String query =
            "CALL gds.alpha.triangles({" +
            "  nodeProjection: '*'," +
            "  relationshipProjection: {" +
            "    TYPE: {" +
            "      type: '*'," +
            "      orientation: 'UNDIRECTED'" +
            "    }" +
            "  }" +
            "})";

        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testKSpanningTreeKMax() {
        String query = GdsCypher.call()
            .loadEverything()
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
    void testShortestPathsDeltaSteppingStream() {
        boolean hasNext = runQuery("MATCH(n:Node {name:'s'}) WITH n CALL gds.alpha.shortestPath.deltaStepping.stream({startNode: n, delta: 0}) YIELD nodeId RETURN nodeId", Result::hasNext);
        assertFalse(hasNext);
    }

    @Test
    void testShortestPathsDeltaStepping() {
        runQueryWithRowConsumer(
            "MATCH(n:Node {name:'s'}) WITH n CALL gds.alpha.shortestPath.deltaStepping.write({startNode: n, delta: 0}) YIELD nodeCount RETURN nodeCount",
            row -> assertEquals(0L, row.getNumber("nodeCount")));
    }
}
