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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.functions.AsNodeFunc;
import org.neo4j.graphdb.Result;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
public class ParticleFilteringProcTest extends BaseProcTest {
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
                    ", (paper3)-[:CITES]->(paper6)" +
                    ", (paper4)-[:CITES]->(paper0)" +
                    ", (paper4)-[:CITES]->(paper1)" +
                    ", (paper4)-[:CITES]->(paper2)" +
                    ", (paper4)-[:CITES]->(paper3)" +
                    ", (paper5)-[:CITES]->(paper1)" +
                    ", (paper5)-[:CITES]->(paper4)" +
                    ", (paper6)-[:CITES]->(paper1)" +
                    ", (paper6)-[:CITES]->(paper4)";

    private static final String NL = System.lineSeparator();

    interface TestConsumer {
        void accept(long nodeId, double centrality);
    }

    @Mock
    private ParticleFilteringProcTest.TestConsumer consumer;

    @BeforeEach
    void setupGraph() throws Exception {
        registerProcedures(ParticleFilteringProc.class);
        registerFunctions(AsNodeFunc.class);
        runQuery(DB_CYPHER);
    }

    @Test
    void testParticleFilteringStream() {
        String query = "MATCH (n:Paper) " +
                "WHERE n.name IN ['Paper 1', 'Paper 2', 'Paper 3']  " +
                "WITH collect(n) AS nodes " +
                "CALL gds.alpha.particleFiltering.stream({sourceNodes:nodes, shuffleNeighbors: false, numberParticles: 10, minimumThreshold: 0.0}) " +
                "YIELD nodeId, score " +
                "RETURN gds.util.asNode(nodeId).name as node, score " +
                "ORDER BY score DESC";

        String expected =
                "+---------------------------------+" + NL +
                "| node      | score               |" + NL +
                "+---------------------------------+" + NL +
                "| \"Paper 0\" | 6.031727701822916   |" + NL +
                "| \"Paper 1\" | 5.558772786458333   |" + NL +
                "| \"Paper 2\" | 3.632161458333333   |" + NL +
                "| \"Paper 3\" | 3.433333333333333   |" + NL +
                "| \"Paper 6\" | 0.7083333333333333  |" + NL +
                "| \"Paper 4\" | 0.30104166666666665 |" + NL +
                "+---------------------------------+" + NL +
                "6 rows" + NL;

        String actual = runQuery(query, Result::resultAsString);
        assertEquals(expected, actual);
    }
}