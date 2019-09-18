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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;

import java.util.HashMap;
import java.util.Map;

public class DegreeProcIssue848Test extends ProcTestBase {

    private static final String DB_CYPHER =
            "UNWIND range(1, 10001) AS s " +
            "CREATE (:Node {id: s})";

    @AfterAll
    public static void tearDown() {
        if (DB != null) DB.shutdown();
    }

    @BeforeAll
    public static void setup() throws KernelException {
        DB = TestDatabaseCreator.createTestDatabase();
        try (Transaction tx = DB.beginTx()) {
            DB.execute(DB_CYPHER).close();
            tx.success();
        }

        DB.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(DegreeCentralityProc.class);
    }

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void multipleBatches(String graphImpl) {
        final Map<Long, Double> actual = new HashMap<>();
        String query = "CALL algo.degree.stream('Node', '', {graph: $graph, direction: 'incoming'}) " +
                       "YIELD nodeId, score";
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> actual.put(
                        (Long)row.get("nodeId"),
                        (Double) row.get("score")));

        Map<Long, Double> expected = new HashMap<>();
        for (long i = 0; i < 10001; i++) {
             expected.put(i, 0.0);
        }

        assertMapEquals(expected, actual);
    }

}
