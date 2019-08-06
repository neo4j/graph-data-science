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

import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.rules.ExpectedException;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;

import static org.junit.Assert.assertEquals;

/**
 *
 */
public class LouvainClusteringPreDefinedCommunitiesProcTest extends ProcTestBase {

    @BeforeAll
    public static void setupGraph() throws KernelException {
        db = TestDatabaseCreator.createTestDatabase();

        final String cypher =
                "  MERGE (nRyan:User {id: 'Ryan'})" +
                "    SET  nRyan.community = 12" +
                "  MERGE (nAlice:User {id: 'Alice'})" +
                "    SET  nAlice.community = 0" +
                "  MERGE (nBridget:User {id: 'Bridget'})" +
                "    SET  nBridget.community = 2" +
                "  MERGE (nMark:User {id: 'Mark'})" +
                "    SET  nMark.community = 10" +
                "  MERGE (nAlice)-[:FRIEND]->(nBridget)";

        db.getDependencyResolver()
                .resolveDependency(Procedures.class)
                .registerProcedure(LouvainProc.class);
        db.execute(cypher);
    }

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    @ParameterizedTest
    @MethodSource("graphImplementations")
    public void testStream(String graphImpl) {
        String query = "CALL algo.louvain.stream(" +
                       "    '', '', {" +
                       "        concurrency: 1, community: 'community', randomNeighbor: false, graph: $graph" +
                       "    }" +
                       ") YIELD nodeId, community, communities";
        IntIntScatterMap testMap = new IntIntScatterMap();
        runQuery(query, MapUtil.map("graph", graphImpl),
                row -> {
                    long community = (long) row.get("community");
                    testMap.addTo((int) community, 1);
                }
        );
        assertEquals(3, testMap.size());
    }
}
