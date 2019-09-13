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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.huge.loader.HugeGraphFactory;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GridBuilder;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.graphalgo.TestDatabaseCreator;

import java.util.concurrent.Executors;

/**
 * The test creates a grid of nodes and computes a reference array
 * of shortest paths using one thread. It then compares the reference
 * against the result of several parallel computations to provoke
 * concurrency errors if any.
 *
 * @author mknblch
 */
public class ParallelDeltaSteppingTest {

    private static final String PROPERTY = "property";
    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    private static GraphDatabaseAPI db;
    private static GridBuilder gridBuilder;
    private static Graph graph;
    private static double[] reference;
    private static long rootNodeId;

    @BeforeClass
    public static void setup() throws Exception {

        db = TestDatabaseCreator.createTestDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms"))) {
            gridBuilder = GraphBuilder.create(db)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newGridBuilder()
                    .createGrid(50, 50)
                    .forEachRelInTx(rel -> {
                        rel.setProperty(PROPERTY, Math.random() * 5); // (0-5)
                    });

            rootNodeId = gridBuilder.getLineNodes()
                    .get(0)
                    .get(0)
                    .getId();
        }

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            graph = new GraphLoader(db)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withRelationshipWeightsFromProperty(PROPERTY, 1.0)
                    .load(HugeGraphFactory.class);
        }

        reference = compute(1);
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }

    @Test
    public void testParallelBehaviour() throws Exception {
        final int n = 20;
        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println(n + "x eval took " + t + "ms"))) {
            for (int i = 0; i < n; i++) {
                Assert.assertArrayEquals("error in iteration " + i,
                        reference,
                        compute((n % 7) + 2),
                        0.001);
            }
        }
    }

    private static double[] compute(int threads) throws Exception {
        return new ShortestPathDeltaStepping(graph, 2.5, Direction.OUTGOING)
                .withExecutorService(Executors.newFixedThreadPool(threads))
                .compute(rootNodeId)
                .getShortestPaths();
    }
}
