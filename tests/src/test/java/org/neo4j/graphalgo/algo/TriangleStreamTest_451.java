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
package org.neo4j.graphalgo.algo;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.TriangleProc;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

/**
 *
 * @author mknblch
 */
public class TriangleStreamTest_451 {

    private static GraphDatabaseAPI DB;

    @BeforeAll
    public static void setup() throws KernelException {
        DB = TestDatabaseCreator.createTestDatabase();
        DB.getDependencyResolver().resolveDependency(Procedures.class).registerProcedure(TriangleProc.class);
    }

    @AfterAll
    static void teardownGraph() {
        DB.shutdown();
    }

    @Test
    public void testEmptySet() {
        DB.execute("CALL algo.triangleCount.stream('', '') YIELD nodeId, triangles");
    }
}
