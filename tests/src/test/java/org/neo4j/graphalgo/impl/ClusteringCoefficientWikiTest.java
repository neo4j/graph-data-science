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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.triangle.IntersectingTriangleCount;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 *
 *   C(x) = (2 * triangles(x)) / (degree(x) * (degree(x) - 1))
 *   C(G) = 1/n * sum( C(x) )
 *
 *
 *   (c)           N |T |L |C
 *   /             --+--+--+----
 * (a)--(b)        a |1 |6 |0.33
 *   \  /          b |1 |2 |1.0
 *   (d)           c |0 |1 |0.0
 *                 d |1 |2 |1.0
 *
 * @author mknblch
 */
public class ClusteringCoefficientWikiTest {

    private static GraphDatabaseAPI db;
    private static Graph graph;

    private static final double[] EXPECTED = {0.33, 1.0, 0.0, 1.0};

    @BeforeClass
    public static void setup() throws Exception {

        db = TestDatabaseCreator.createTestDatabase();

        final String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                        "CREATE (b:Node {name:'b'})\n" +
                        "CREATE (c:Node {name:'c'})\n" +
                        "CREATE (d:Node {name:'d'})\n" +
                        "CREATE" +
                        " (a)-[:TYPE]->(b),\n" +
                        " (a)-[:TYPE]->(c),\n" +
                        " (a)-[:TYPE]->(d),\n" +
                        " (b)-[:TYPE]->(d)";

        try (Transaction tx = db.beginTx()) {
            db.execute(cypher);
            tx.success();
        }

        graph = new GraphLoader(db)
                .withAnyLabel()
                .withAnyRelationshipType()
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
        final IntersectingTriangleCount algo =
                new IntersectingTriangleCount(graph, Pools.DEFAULT, 4, AllocationTracker.EMPTY)
                        .compute();
        assertArrayEquals(EXPECTED, algo.getCoefficients().toArray(), 0.1);
        assertEquals(0.583, algo.getAverageCoefficient(), 0.01);
    }
}
