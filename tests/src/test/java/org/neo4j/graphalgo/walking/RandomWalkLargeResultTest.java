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
package org.neo4j.graphalgo.walking;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.NodeWalkerProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RandomWalkLargeResultTest {

    private static final int NODE_COUNT = 20000;

    private GraphDatabaseAPI db;
    private Transaction tx;

    @BeforeEach
    void beforeClass() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(NodeWalkerProc.class);
        db.execute(buildDatabaseQuery(), Collections.singletonMap("count",NODE_COUNT)).close();
        tx = db.beginTx();
    }

    @AfterEach
    void AfterClass() {
        tx.close();
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
        Result results = db.execute("CALL algo.randomWalk.stream(null, 100, 100000)");
        assertEquals(100000,Iterators.count(results));
    }
}
