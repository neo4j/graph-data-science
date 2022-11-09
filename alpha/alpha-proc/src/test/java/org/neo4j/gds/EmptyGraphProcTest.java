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
import org.neo4j.gds.centrality.HarmonicCentralityStreamProc;
import org.neo4j.gds.centrality.HarmonicCentralityWriteProc;
import org.neo4j.gds.scc.SccStreamProc;
import org.neo4j.gds.scc.SccWriteProc;
import org.neo4j.gds.shortestpaths.AllShortestPathsProc;
import org.neo4j.gds.triangle.TriangleProc;

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
            SccStreamProc.class,
            SccWriteProc.class,
            TriangleProc.class,
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

}
