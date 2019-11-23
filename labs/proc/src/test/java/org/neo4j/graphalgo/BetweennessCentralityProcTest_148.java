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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Matchers;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BetweennessCentralityProcTest_148 extends ProcTestBase {

    @BeforeEach
    void setupGraph() throws KernelException {

        db = TestDatabaseCreator.createTestDatabase();

        registerProcedures(BetweennessCentralityProc.class);

        final String importQuery =
                "CREATE (nAlice:User {name:'Alice'})\n" +
                        ",(nBridget:User {name:'Bridget'})\n" +
                        ",(nCharles:User {name:'Charles'})\n" +
                        ",(nDoug:User {name:'Doug'})\n" +
                        ",(nMark:User {name:'Mark'})\n" +
                        "CREATE (nAlice)-[:FRIEND]->(nBridget)\n" +
                        ",(nCharles)-[:FRIEND]->(nBridget)\n" +
                        ",(nDoug)-[:FRIEND]->(nBridget)\n" +
                        ",(nMark)-[:FRIEND]->(nBridget)\n" +
                        ",(nMark)-[:FRIEND]->(nDoug)\n";

        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.printf("Setup took %d ms%n", l))) {
            try (Transaction tx = db.beginTx()) {
                db.execute(importQuery);
                tx.success();
            }
        }

    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    private String name(long id) {
        String[] name = {""};
        db.execute("MATCH (n) WHERE id(n) = " + id + " RETURN n.name as name")
                .accept(row -> {
                    name[0] = row.getString("name");
                    return false;
                });
        if (name[0].isEmpty()) {
            throw new IllegalArgumentException("unknown id " + id);
        }
        return name[0];
    }

    @Test
    void testBCStreamDirectionBoth() throws Exception {

        final Consumer mock = mock(Consumer.class);
        final String evalQuery = "CALL algo.betweenness.stream('User', 'FRIEND', {direction:'B'}) YIELD nodeId, centrality";
        db.execute(evalQuery).accept(row -> {
            final long nodeId = row.getNumber("nodeId").longValue();
            final double centrality = row.getNumber("centrality").doubleValue();
            mock.consume(name(nodeId), centrality);
            return true;
        });

        verify(mock, times(4)).consume(Matchers.anyString(), AdditionalMatchers.eq(0.0, 0.1));
        verify(mock, times(1)).consume(Matchers.eq("Bridget"), AdditionalMatchers.eq(5.0, 0.1));
    }

    interface Consumer {
        void consume(String name, double centrality);
    }
}
