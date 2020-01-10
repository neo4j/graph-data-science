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
package org.neo4j.graphalgo;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.helpers.collection.Iterators;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RandomWalkLargeResultProcTest extends BaseProcTest {

    private static final int NODE_COUNT = 20000;

    @BeforeEach
    void beforeClass() throws Exception {
        db = TestDatabaseCreator.createTestDatabase();
        registerProcedures(NodeWalkerProc.class);
        runQuery(buildDatabaseQuery(), Collections.singletonMap("count",NODE_COUNT));
    }

    @AfterEach
    void AfterClass() {
        db.shutdown();
    }

    private static String buildDatabaseQuery() {
        return "UNWIND range(0,$count) as id " +
                "CREATE (n:Node) " +
                "WITH collect(n) as nodes " +
                "unwind nodes as n with n, nodes[toInteger(rand()*10000)] as m " +
                "create (n)-[:FOO]->(m)";
    }

    @Test
    void shouldHandleLargeResults() {
        long resultsCount = runQuery("CALL algo.randomWalk.stream(null, 100, 100000)", Iterators::count);
        assertEquals(100000, resultsCount);
    }
}
