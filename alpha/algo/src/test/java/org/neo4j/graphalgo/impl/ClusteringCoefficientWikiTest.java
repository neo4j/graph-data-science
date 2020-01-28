/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
import org.neo4j.graphalgo.Projection;
import org.neo4j.graphalgo.StoreLoaderBuilder;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.triangle.IntersectingTriangleCount;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
 */
class ClusteringCoefficientWikiTest extends AlgoTestBase {

    private Graph graph;

    private static final double[] EXPECTED = {0.33, 1.0, 0.0, 1.0};

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();

        String cypher =
                "CREATE (a:Node {name:'a'})\n" +
                "CREATE (b:Node {name:'b'})\n" +
                "CREATE (c:Node {name:'c'})\n" +
                "CREATE (d:Node {name:'d'})\n" +
                "CREATE" +
                " (a)-[:TYPE]->(b),\n" +
                " (a)-[:TYPE]->(c),\n" +
                " (a)-[:TYPE]->(d),\n" +
                " (b)-[:TYPE]->(d)";

        runQuery(cypher);

        graph = new StoreLoaderBuilder()
            .api(db)
            .loadAnyLabel()
            .loadAnyRelationshipType()
            .globalProjection(Projection.UNDIRECTED)
            .build()
            .graph(HugeGraphFactory.class);
    }

    @AfterEach
    void tearDown() {
        db.shutdown();
    }


    @Test
    void test() {
        final IntersectingTriangleCount algo =
            new IntersectingTriangleCount(graph, Pools.DEFAULT, 4, AllocationTracker.EMPTY);
        algo.compute();
        assertArrayEquals(EXPECTED, algo.getCoefficients().toArray(), 0.1);
        assertEquals(0.583, algo.getAverageCoefficient(), 0.01);
    }
}
