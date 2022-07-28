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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.centrality.HarmonicCentralityStreamProc;
import org.neo4j.gds.centrality.HarmonicCentralityWriteProc;
import org.neo4j.gds.influenceMaximization.CELFStreamProc;
import org.neo4j.gds.influenceMaximization.GreedyProc;
import org.neo4j.gds.scc.SccStreamProc;
import org.neo4j.gds.scc.SccWriteProc;
import org.neo4j.gds.shortestpaths.AllShortestPathsProc;
import org.neo4j.gds.spanningtree.KSpanningTreeMaxProc;
import org.neo4j.gds.spanningtree.KSpanningTreeMinProc;
import org.neo4j.gds.spanningtree.SpanningTreeProcMax;
import org.neo4j.gds.spanningtree.SpanningTreeProcMin;
import org.neo4j.gds.triangle.TriangleProc;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class EmptyGraphProcTest extends BaseProcTest {

    private static final String GRAPH_NAME = "graph";

    @BeforeEach
    void setup() throws Exception {
        registerProcedures(
            AllShortestPathsProc.class,
            HarmonicCentralityStreamProc.class,
            HarmonicCentralityWriteProc.class,
            KSpanningTreeMaxProc.class,
            KSpanningTreeMinProc.class,
            SpanningTreeProcMax.class,
            SpanningTreeProcMin.class,
            SccStreamProc.class,
            SccWriteProc.class,
            TriangleProc.class,
            GreedyProc.class,
            CELFStreamProc.class,
            GraphProjectProc.class
        );

        var createQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .loadEverything()
            .yields();
        runQuery(createQuery);
    }

    @Test
    void testSccStream() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.scc")
            .streamMode()
            .yields();
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testSccWrite() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.scc")
            .writeMode()
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testAllShortestPathsStream() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.allShortestPaths")
            .streamMode()
            .yields();
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testHarmonicCentralityWrite() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.closeness.harmonic")
            .writeMode()
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("nodes")));
    }

    @Test
    void testTriangleStream() {
        var createQuery = GdsCypher.call("undirectedGraph")
            .graphProject()
            .loadEverything(Orientation.UNDIRECTED)
            .yields();
        runQuery(createQuery);

        String query = "CALL gds.alpha.triangles('undirectedGraph', {})";
        runQueryWithResultConsumer(query, result -> assertFalse(result.hasNext()));
    }

    @Test
    void testKSpanningTreeKMax() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.spanningTree.kmax")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("k", 3)
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testKSpanningTreeKMin() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.spanningTree.kmin")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("k", 3)
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testSpanningTree() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.spanningTree")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("weightWriteProperty", "weight")
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testSpanningTreeMinimum() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.spanningTree.minimum")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("weightWriteProperty", "weight")
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    void testSpanningTreeMaximum() {
        String query = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.spanningTree.maximum")
            .writeMode()
            .addParameter("startNodeId", 0)
            .addParameter("weightWriteProperty", "weight")
            .yields();
        runQueryWithRowConsumer(query, row -> assertEquals(0L, row.getNumber("effectiveNodeCount")));
    }

    @Test
    @Disabled
    void testShortestPathsDeltaSteppingStream() {
        boolean hasNext = runQuery("MATCH(n:Node {name:'s'}) WITH n CALL gds.alpha.shortestPath.deltaStepping.stream({startNode: n, delta: 0}) YIELD nodeId RETURN nodeId", Result::hasNext);
        assertFalse(hasNext);
    }

    @Test
    @Disabled
    void testShortestPathsDeltaStepping() {
        runQueryWithRowConsumer(
            "MATCH(n:Node {name:'s'}) WITH n CALL gds.alpha.shortestPath.deltaStepping.write({startNode: n, delta: 0}) YIELD nodeCount RETURN nodeCount",
            row -> assertEquals(0L, row.getNumber("nodeCount")));
    }
}
