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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.AlgoTestBase;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.triangle.IntersectingTriangleCount;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
 */
class ClusteringCoefficientTest extends AlgoTestBase {

    private static final double[] EXPECTED = {
        1.0, 1.0, 3.0/10, 1.0, 1.0, 2.0/3
    };

    private static final String LABEL = "Node";

    private static Graph graph;

    @BeforeEach
    void setup() {

        String setupCypher =
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

        runQuery(setupCypher);

        graph = new GraphLoader(db)
                .withLabel(LABEL)
                .undirected()
                .load(HugeGraphFactory.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
        graph = null;
    }


    @Test
    void test() {
        IntersectingTriangleCount algo =
            new IntersectingTriangleCount(graph, Pools.DEFAULT, 4, AllocationTracker.EMPTY)
                .compute();
        assertArrayEquals(EXPECTED, algo.getCoefficients().toArray(), 0.01);
        assertEquals(0.827, algo.getAverageCoefficient(), 0.01);
    }
}
