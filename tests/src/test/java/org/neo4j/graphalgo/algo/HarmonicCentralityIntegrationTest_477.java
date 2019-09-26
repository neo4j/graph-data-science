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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.graphalgo.HarmonicCentralityProc;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

@ExtendWith(MockitoExtension.class)
public class HarmonicCentralityIntegrationTest_477 {

    private static GraphDatabaseAPI db;

    @BeforeAll
    static void setupGraph() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();
        db.execute(
                  "CREATE (alice:Person{id:\"Alice\"}),\n" +
                    "       (michael:Person{id:\"Michael\"}),\n" +
                    "       (karin:Person{id:\"Karin\"}),\n" +
                    "       (chris:Person{id:\"Chris\"}),\n" +
                    "       (will:Person{id:\"Will\"}),\n" +
                    "       (mark:Person{id:\"Mark\"})\n" +
                    "CREATE (michael)-[:KNOWS]->(karin),\n" +
                    "       (michael)-[:KNOWS]->(chris),\n" +
                    "       (will)-[:KNOWS]->(michael),\n" +
                    "       (mark)-[:KNOWS]->(michael),\n" +
                    "       (mark)-[:KNOWS]->(will),\n" +
                    "       (alice)-[:KNOWS]->(michael),\n" +
                    "       (will)-[:KNOWS]->(chris),\n" +
                    "       (chris)-[:KNOWS]->(karin);"
        );

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(HarmonicCentralityProc.class);
    }

    @Test
    void testLoad() {
        String cypher =
                "CALL algo.closeness.harmonic.stream(" +
                "    'MATCH (u:Person) RETURN id(u) as id'," +
                "    'MATCH (u1:Person)-[k:KNOWS]-(u2:Person) RETURN id(u1) AS source, id(u2) AS target', {" +
                "           graph: 'cypher'" +
                "    }" +
                ") YIELD nodeId, centrality   ";

        db.execute(cypher);
    }

}
