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
package org.neo4j.gds.pagerank;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.catalog.GraphProjectProc;
import org.neo4j.gds.extension.Neo4jGraph;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.isA;

public class PageRankStatsProcTest extends BaseProcTest {

    @Neo4jGraph
    public static final String DB_CYPHER =
        "CREATE" +
        "  (a:Label1 {name: 'a'})" +
        ", (b:Label1 {name: 'b'})" +
        ", (c:Label1 {name: 'c'})" +
        ", (d:Label1 {name: 'd'})" +
        ", (e:Label1 {name: 'e'})" +
        ", (f:Label1 {name: 'f'})" +
        ", (g:Label1 {name: 'g'})" +
        ", (h:Label1 {name: 'h'})" +
        ", (i:Label1 {name: 'i'})" +
        ", (j:Label1 {name: 'j'})" +
        ", (k:Label2 {name: 'k'})" +
        ", (l:Label2 {name: 'l'})" +
        ", (m:Label2 {name: 'm'})" +
        ", (n:Label2 {name: 'n'})" +
        ", (o:Label2 {name: 'o'})" +
        ", (p:Label2 {name: 'p'})" +
        ", (q:Label2 {name: 'q'})" +
        ", (r:Label2 {name: 'r'})" +
        ", (s:Label2 {name: 's'})" +
        ", (t:Label2 {name: 't'})" +
        ", (u:Label3 {name: 'u'})" +
        ", (v:Label3 {name: 'v'})" +
        ", (w:Label3 {name: 'w'})" +
        ", (b)-[:TYPE1 {weight: 1.0,  equalWeight: 1.0}]->(c)" +
        ", (c)-[:TYPE1 {weight: 1.2,  equalWeight: 1.0}]->(b)" +
        ", (d)-[:TYPE1 {weight: 1.3,  equalWeight: 1.0}]->(a)" +
        ", (d)-[:TYPE1 {weight: 1.7,  equalWeight: 1.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 6.1,  equalWeight: 1.0}]->(b)" +
        ", (e)-[:TYPE1 {weight: 2.2,  equalWeight: 1.0}]->(d)" +
        ", (e)-[:TYPE1 {weight: 1.5,  equalWeight: 1.0}]->(f)" +
        ", (f)-[:TYPE1 {weight: 10.5, equalWeight: 1.0}]->(b)" +
        ", (f)-[:TYPE1 {weight: 2.9,  equalWeight: 1.0}]->(e)" +
        ", (g)-[:TYPE2 {weight: 3.2,  equalWeight: 1.0}]->(b)" +
        ", (g)-[:TYPE2 {weight: 5.3,  equalWeight: 1.0}]->(e)" +
        ", (h)-[:TYPE2 {weight: 9.5,  equalWeight: 1.0}]->(b)" +
        ", (h)-[:TYPE2 {weight: 0.3,  equalWeight: 1.0}]->(e)" +
        ", (i)-[:TYPE2 {weight: 5.4,  equalWeight: 1.0}]->(b)" +
        ", (i)-[:TYPE2 {weight: 3.2,  equalWeight: 1.0}]->(e)" +
        ", (j)-[:TYPE2 {weight: 9.5,  equalWeight: 1.0}]->(e)" +
        ", (k)-[:TYPE2 {weight: 4.2,  equalWeight: 1.0}]->(e)" +
        ", (u)-[:TYPE3 {weight: 1.0}]->(v)" +
        ", (u)-[:TYPE3 {weight: 1.0}]->(w)" +
        ", (v)-[:TYPE3 {weight: 1.0}]->(w)";

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(GraphProjectProc.class, PageRankStatsProc.class);
        runQuery("CALL gds.graph.project('graph', '*', '*')");
    }

    @Test
    void testStatsYields() {
        String query = GdsCypher
            .call("graph")
            .algo("pageRank")
            .statsMode()
            .addParameter("tolerance", 0.1)
            .yields();

        assertCypherResult(query, List.of(Map.of(
            "preProcessingMillis", greaterThan(-1L),
            "computeMillis", greaterThan(-1L),
            "postProcessingMillis", greaterThan(-1L),
            "didConverge", true,
            "ranIterations", 13L,
            "centralityDistribution", isA(Map.class),
            "configuration", isA(Map.class)
        )));
    }

}
