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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.core.neo4jview.GraphViewFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.closeness.MSClosenessCentrality;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertArrayEquals;

/**
 * Graph:
 *
 *  (A)<-->(B)<-->(C)<-->(D)<-->(E)
 *
 * Calculation:
 *
 * N = 5        // number of nodes
 * k = N-1 = 4  // used for normalization
 *
 *      A     B     C     D     E
 *  --|-----------------------------
 *  A | 0     1     2     3     4       // farness between each pair of nodes
 *  B | 1     0     1     2     3
 *  C | 2     1     0     1     2
 *  D | 3     2     1     0     1
 *  E | 4     3     2     1     0
 *  --|-----------------------------
 *  S | 10    7     6     7     10      // sum each column
 *  ==|=============================
 * k/S| 0.4  0.57  0.67  0.57   0.4     // normalized centrality
 *
 * @author mknblch
 */
@RunWith(Parameterized.class)
public class ClosenessCentralityTest {

    private static final double[] EXPECTED = new double[]{0.4, 0.57, 0.66, 0.57, 0.4};

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{HeavyGraphFactory.class, "Heavy"},
                new Object[]{HugeGraphFactory.class, "Huge"},
                new Object[]{GraphViewFactory.class, "View"}
        );
    }

    @BeforeClass
    public static void setupGraph() throws KernelException {
        DB.execute("CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE (e:Node {name:'e'})\n" +
                "CREATE" +
                " (a)-[:TYPE]->(b),\n" +
                " (b)-[:TYPE]->(a),\n" +
                " (b)-[:TYPE]->(c),\n" +
                " (c)-[:TYPE]->(b),\n" +
                " (c)-[:TYPE]->(d),\n" +
                " (d)-[:TYPE]->(c),\n" +
                " (d)-[:TYPE]->(e),\n" +
                " (e)-[:TYPE]->(d)");
    }

    private Graph graph;

    public ClosenessCentralityTest(
            Class<? extends GraphFactory> graphImpl,
            String nameIgnoredOnlyForTestName) {
        graph = new GraphLoader(DB)
                .withAnyRelationshipType()
                .withAnyLabel()
                .withoutNodeProperties()
                .load(graphImpl);
    }

    @Test
    public void testGetCentrality() throws Exception {
        final double[] centrality =
                new MSClosenessCentrality(
                        graph,
                        AllocationTracker.EMPTY,
                        Pools.DEFAULT_CONCURRENCY,
                        Pools.DEFAULT,
                        false)
                        .compute()
                        .exportToArray();

        assertArrayEquals(EXPECTED, centrality, 0.1);
    }

    @Test
    public void testStream() throws Exception {
        final double[] centrality = new double[(int) graph.nodeCount()];

        new MSClosenessCentrality(graph, AllocationTracker.EMPTY, Pools.DEFAULT_CONCURRENCY, Pools.DEFAULT, false)
                .compute()
                .resultStream()
                .forEach(r -> centrality[Math.toIntExact(graph.toMappedNodeId(r.nodeId))] = r.centrality);

        assertArrayEquals(EXPECTED, centrality, 0.1);
    }
}
