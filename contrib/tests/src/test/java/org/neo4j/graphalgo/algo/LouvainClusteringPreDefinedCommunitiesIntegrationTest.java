/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.LouvainProc;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 *
 */
@RunWith(Parameterized.class)
public class LouvainClusteringPreDefinedCommunitiesIntegrationTest {

    @ClassRule
    public static ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @BeforeClass
    public static void setupGraph() throws KernelException {

        final String cypher =
                "MERGE (nRyan:User {id:'Ryan'}) SET nRyan.community = 12\n" +
                "MERGE (nAlice:User {id:'Alice'}) SET nAlice.community = 0\n" +
                "MERGE (nBridget:User {id:'Bridget'}) SET nBridget.community = 2\n" +
                "MERGE (nMark:User {id:'Mark'}) SET nMark.community = 10\n" +
                "MERGE (nAlice)-[:FRIEND]->(nBridget);";

        DB.resolveDependency(Procedures.class).registerProcedure(LouvainProc.class);
        DB.execute(cypher);
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"heavy"},
                new Object[]{"huge"}
        );
    }

    @Parameterized.Parameter
    public String graphImpl;

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    @Test
    public void testStream() {
        final String cypher = "CALL algo.louvain.stream('', '', {concurrency:1, community: 'community', randomNeighbor:false, graph:$graph}) " +
                "YIELD nodeId, community, communities";
        final IntIntScatterMap testMap = new IntIntScatterMap();
        DB.execute(cypher, MapUtil.map("graph", graphImpl)).accept(row -> {
            final long community = (long) row.get("community");
            testMap.addTo((int) community, 1);
            return false;
        });
        assertEquals(3, testMap.size());
    }
}
