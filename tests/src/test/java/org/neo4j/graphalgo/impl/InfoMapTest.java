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
package org.neo4j.graphalgo.impl;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.TestProgressLogger;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.impl.infomap.InfoMap;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;

/**
 * --------- Graph 3x2 ---------
 *
 *        (b)        (e)
 *       /  \       /  \    (x)
 *     (a)--(c)---(d)--(f)
 *
 * --------- Graph 4x2 ---------
 *
 *      (a)-(b)---(e)-(f)
 *       | X |     | X |    (z)
 *      (c)-(d)   (g)-(h)
 *
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class InfoMapTest {

    @ClassRule
    public static ImpermanentDatabaseRule db = new ImpermanentDatabaseRule();

    private static Graph graph;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{CYPHER_2x3},
                new Object[]{CYPHER_2x4}
        );
    }

    private static final String CYPHER_2x4 =
            "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (c:Node {name:'c'})\n" + // shuffled
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (d:Node {name:'d'})\n" +
                    "CREATE (e:Node {name:'e'})\n" +
                    "CREATE (g:Node {name:'g'})\n" +
                    "CREATE (f:Node {name:'f'})\n" +
                    "CREATE (h:Node {name:'h'})\n" +
                    "CREATE (z:Node {name:'z'})\n" +

                    "CREATE" +

                    " (a)-[:TYPE]->(b),\n" +
                    " (a)-[:TYPE]->(c),\n" +
                    " (a)-[:TYPE]->(d),\n" +
                    " (b)-[:TYPE]->(c),\n" +
                    " (c)-[:TYPE]->(d),\n" +
                    " (b)-[:TYPE]->(d),\n" +

                    " (f)-[:TYPE]->(e),\n" +
                    " (e)-[:TYPE]->(h),\n" +
                    " (e)-[:TYPE]->(g),\n" +
                    " (f)-[:TYPE]->(g),\n" +
                    " (f)-[:TYPE]->(h),\n" +
                    " (g)-[:TYPE]->(h),\n" +
                    " (b)-[:TYPE]->(e)";

    private static final String CYPHER_2x3 =
            "CREATE (a:Node {name:'a'})\n" +
                    "CREATE (b:Node {name:'b'})\n" +
                    "CREATE (c:Node {name:'c'})\n" +
                    "CREATE (d:Node {name:'d'})\n" +
                    "CREATE (e:Node {name:'e'})\n" +
                    "CREATE (f:Node {name:'f'})\n" +
                    "CREATE (x:Node {name:'x'})\n" +
                    "CREATE" +
                    " (b)-[:TYPE]->(a),\n" +
                    " (a)-[:TYPE]->(c),\n" +
                    " (c)-[:TYPE]->(a),\n" +

                    " (d)-[:TYPE]->(c),\n" +

                    " (d)-[:TYPE]->(e),\n" +
                    " (d)-[:TYPE]->(f),\n" +
                    " (e)-[:TYPE]->(f)";


    public InfoMapTest(String cypher) {
        this.cypher = cypher;
    }

    private final String cypher;

    @Before
    public void setupGraph() throws KernelException {

        db.execute("MATCH (n) detach delete n");
        db.execute(cypher);

        graph = new GraphLoader(db)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .asUndirected(true)
                .load(HeavyGraphFactory.class);

    }

    @Test
    public void testClustering() throws Exception {

        // trigger parallel exec on small set size
        InfoMap.MIN_MODS_PARALLEL_EXEC = 2;

        // do it!
        final InfoMap algo = InfoMap.unweighted(
                graph,
                10,
                InfoMap.THRESHOLD,
                InfoMap.TAU,
                Pools.FJ_POOL,
                4, TestProgressLogger.INSTANCE, TerminationFlag.RUNNING_TRUE
        ).compute();

        // should be 3 communities in each graph
        assertEquals(3, algo.getCommunityCount());

        System.out.printf("%27s | %d iterations%n",
                Arrays.toString(algo.getCommunities()),
                algo.getIterations());
    }

}
