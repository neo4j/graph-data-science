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
package org.neo4j.graphalgo.centrality;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.util.HashMap;
import java.util.Map;

class DegreeCentralityProcIssue848Test extends BaseProcTest {

    private static final String DB_CYPHER =
            "UNWIND range(1, 10001) AS s " +
            "CREATE (:Node {id: s})";

    @BeforeEach
    void setup() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        runQuery(DB_CYPHER);
        registerProcedures(DegreeCentralityProc.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }

    @Test
    void multipleBatches() {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL gds.alpha.degree.stream({nodeProjection: 'Node', relationshipProjection: '', direction: 'OUTGOING'}) " +
                       "YIELD nodeId, score";
        runQuery(query, row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        Map<Long, Double> expected = new HashMap<>();
        for (long i = 0; i < 10001; i++) {
             expected.put(i, 0.0);
        }

        assertMapEquals(expected, actual);
    }

}
