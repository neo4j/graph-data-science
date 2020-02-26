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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Matchers;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.TestDatabaseCreator;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BetweennessCentralityProcTest_148 extends BaseProcTest {

    public static final String DB_CYPHER =
        "CREATE " +
        "  (nAlice:User {name:'Alice'})" +
        ", (nBridget:User {name:'Bridget'})" +
        ", (nCharles:User {name:'Charles'})" +
        ", (nDoug:User {name:'Doug'})" +
        ", (nMark:User {name:'Mark'})" +
        ", (nAlice)-[:FRIEND]->(nBridget)" +
        ", (nCharles)-[:FRIEND]->(nBridget)" +
        ", (nDoug)-[:FRIEND]->(nBridget)" +
        ", (nMark)-[:FRIEND]->(nBridget)" +
        ", (nMark)-[:FRIEND]->(nDoug)";

    @BeforeEach
    void setupGraph() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(BetweennessCentralityProc.class);
        runQuery(DB_CYPHER);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    private String name(long id) {
        String[] name = {""};
        runQueryWithRowConsumer(
            "MATCH (n) WHERE id(n) = " + id + " RETURN n.name as name",
            row -> name[0] = row.getString("name")
        );
        if (name[0].isEmpty()) {
            throw new IllegalArgumentException("unknown id " + id);
        }
        return name[0];
    }

    @Test
    void testBCStreamUndirected() {
        Consumer mock = mock(Consumer.class);
        String query = GdsCypher.call()
            .withNodeLabel("User")
            .withRelationshipType("FRIEND", Orientation.UNDIRECTED)
            .algo("gds.alpha.betweenness")
            .streamMode()
            .yields("nodeId", "centrality");
        runQueryWithRowConsumer(query, row -> {
            long nodeId = row.getNumber("nodeId").longValue();
            double centrality = row.getNumber("centrality").doubleValue();
            mock.consume(name(nodeId), centrality);
        });

        verify(mock, times(4)).consume(Matchers.anyString(), AdditionalMatchers.eq(0.0, 0.1));
        verify(mock, times(1)).consume(Matchers.eq("Bridget"), AdditionalMatchers.eq(5.0, 0.1));
    }

    interface Consumer {
        void consume(String name, double centrality);
    }
}
