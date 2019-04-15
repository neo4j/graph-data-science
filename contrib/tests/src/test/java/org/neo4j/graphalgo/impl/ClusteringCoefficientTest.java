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
package org.neo4j.graphalgo.impl;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.impl.triangle.TriangleCountQueue;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 * Example: <a href="http://cs.stanford.edu/~rishig/courses/ref/l1.pdf">http://cs.stanford.edu/~rishig/courses/ref/l1.pdf</a>
 *
 * <pre>
 *
 * (a)    (d)
 *  | \  /   \
 *  | (c)----(f)
 *  | /  \   /
 * (b)    (e)
 *
 *  N |T |D |L |C
 *  --+--+--+--+---
 *  a |1 |2 |2 |1.0
 *  b |1 |2 |2 |1.0
 *  c |3 |5 |20|3/10
 *  d |1 |2 |2 |1.0
 *  e |1 |2 |2 |1.0
 *  f |2 |3 |6 |2/3
 * </pre>
 *
 * @author mknblch
 */
public class ClusteringCoefficientTest {

    private static final double[] EXPECTED = {
        1.0, 1.0, 3.0/10, 1.0, 1.0, 2.0/3
    };

    private static final String LABEL = "Node";

    private static GraphDatabaseAPI db;
    private static Graph graph;

    @BeforeClass
    public static void setup() throws Exception {

        final String setupCypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE (e:Node {name:'e'})\n" +
                        "CREATE (f:Node {name:'f'})\n" +

                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (b)-[:TYPE]->(c),\n" +

                        " (c)-[:TYPE]->(e),\n" +
                        " (e)-[:TYPE]->(f),\n" +

                        " (c)-[:TYPE]->(d),\n" +
                        " (c)-[:TYPE]->(f),\n" +
                        " (d)-[:TYPE]->(f)";

        db = TestDatabaseCreator.createTestDatabase();

        try (Transaction tx = db.beginTx()) {
            db.execute(setupCypher);
            tx.success();
        }

        graph = new GraphLoader(db)
                .withLabel(LABEL)
                .withoutRelationshipWeights()
                .withoutNodeWeights()
                .asUndirected(true)
                .load(HeavyGraphFactory.class);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (db != null) db.shutdown();
        graph = null;
    }


    @Test
    public void test() throws Exception {

        final TriangleCountQueue algo =
                new TriangleCountQueue(graph, Pools.DEFAULT, 4)
                        .compute();

        assertArrayEquals(EXPECTED, algo.getCoefficients(), 0.01);
        assertEquals(0.827, algo.getAverageCoefficient(), 0.01);
    }
}
