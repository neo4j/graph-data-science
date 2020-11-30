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

package org.neo4j.graphalgo.centrality;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.functions.AsNodeFunc;
import org.neo4j.graphdb.Result;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ArticleRankProcTest extends BaseProcTest {

    private static final String DB_CYPHER =
        "CREATE" +
        "  (paper0:Paper {name:'Paper 0'})" +
        ", (paper1:Paper {name:'Paper 1'})" +
        ", (paper2:Paper {name:'Paper 2'})" +
        ", (paper3:Paper {name:'Paper 3'})" +
        ", (paper4:Paper {name:'Paper 4'})" +
        ", (paper5:Paper {name:'Paper 5'})" +
        ", (paper6:Paper {name:'Paper 6'})" +
        ", (paper1)-[:CITES]->(paper0)" +
        ", (paper2)-[:CITES]->(paper0)" +
        ", (paper2)-[:CITES]->(paper1)" +
        ", (paper3)-[:CITES]->(paper0)" +
        ", (paper3)-[:CITES]->(paper1)" +
        ", (paper3)-[:CITES]->(paper2)" +
        ", (paper4)-[:CITES]->(paper0)" +
        ", (paper4)-[:CITES]->(paper1)" +
        ", (paper4)-[:CITES]->(paper2)" +
        ", (paper4)-[:CITES]->(paper3)" +
        ", (paper5)-[:CITES]->(paper1)" +
        ", (paper5)-[:CITES]->(paper4)" +
        ", (paper6)-[:CITES]->(paper1)" +
        ", (paper6)-[:CITES]->(paper4)";

    private static final String NL = System.lineSeparator();

    @BeforeEach
    void setUp() throws Exception {
        registerProcedures(ArticleRankProc.class);
        registerFunctions(AsNodeFunc.class);
        runQuery(DB_CYPHER);
    }

    @Test
    void shouldStream() {
        String query = GdsCypher.call()
            .withNodeLabel("Paper")
            .withRelationshipType("CITES")
            .algo("gds.alpha.articleRank")
            .streamMode()
            .addParameter("maxIterations", 20)
            .addParameter("dampingFactor", 0.85)
            .addParameter("concurrency", 1)
            .yields("nodeId", "score")
            .concat(" RETURN gds.util.asNode(nodeId).name AS page, score")
            .concat(" ORDER BY score DESC");

        String expected =
            "+---------------------------------+" + NL +
            "| page      | score               |" + NL +
            "+---------------------------------+" + NL +
            "| \"Paper 0\" | 0.7561931175296194  |" + NL +
            "| \"Paper 1\" | 0.5640594220254571  |" + NL +
            "| \"Paper 2\" | 0.306357501167804   |" + NL +
            "| \"Paper 4\" | 0.27750000506639483 |" + NL +
            "| \"Paper 3\" | 0.22862500101327898 |" + NL +
            "| \"Paper 5\" | 0.15000000000000002 |" + NL +
            "| \"Paper 6\" | 0.15000000000000002 |" + NL +
            "+---------------------------------+" + NL +
            "7 rows" + NL;

        String actual = runQuery(query, Result::resultAsString);
        assertEquals(expected, actual);
    }

    @Test
    void shouldWrite() {
        String algoQuery = GdsCypher.call()
            .withNodeLabel("Paper")
            .withRelationshipType("CITES")
            .algo("gds.alpha.articleRank")
            .writeMode()
            .addParameter("maxIterations", 20)
            .addParameter("dampingFactor", 0.85)
            .addParameter("concurrency", 1)
            .yields();

        String resultQuery =
            " MATCH (n)" +
            " RETURN n.name AS page, n.articlerank AS score" +
            " ORDER BY score DESC";

        String expected =
            "+---------------------------------+" + NL +
            "| page      | score               |" + NL +
            "+---------------------------------+" + NL +
            "| \"Paper 0\" | 0.7561931175296194  |" + NL +
            "| \"Paper 1\" | 0.5640594220254571  |" + NL +
            "| \"Paper 2\" | 0.306357501167804   |" + NL +
            "| \"Paper 4\" | 0.27750000506639483 |" + NL +
            "| \"Paper 3\" | 0.22862500101327898 |" + NL +
            "| \"Paper 5\" | 0.15000000000000002 |" + NL +
            "| \"Paper 6\" | 0.15000000000000002 |" + NL +
            "+---------------------------------+" + NL +
            "7 rows" + NL;

        runQueryWithRowConsumer(algoQuery, row -> {
            assertNotEquals(-1L, row.getNumber("createMillis").longValue());
            assertNotEquals(-1L, row.getNumber("computeMillis").longValue());
            assertNotEquals(-1L, row.getNumber("writeMillis").longValue());
            Map<String, Object> centralityDistribution = (Map<String, Object>) row.get("centralityDistribution");
            assertNotNull(centralityDistribution);
            assertEquals(0.756, (Double) centralityDistribution.get("max"), 1e-2);
        });

        String actual = runQuery(resultQuery, Result::resultAsString);
        assertEquals(expected, actual);
    }

}
