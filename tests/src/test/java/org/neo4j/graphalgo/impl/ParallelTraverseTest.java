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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.traverse.ParallelLocalQueueBFS;
import org.neo4j.graphalgo.helper.graphbuilder.GraphBuilder;
import org.neo4j.graphalgo.helper.graphbuilder.GridBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author mknblch
 */
@RunWith(MockitoJUnitRunner.class)
public class ParallelTraverseTest {


    private static final String PROPERTY = "property";
    private static final String LABEL = "Node";
    private static final String RELATIONSHIP = "REL";

    private static GraphDatabaseAPI db;
    private static GridBuilder gridBuilder;
    private static Graph graph;
    private static long rootNodeId;
    private static int nodeCount;

    @Mock
    LongConsumer mock;

    @BeforeClass
    public static void setup() throws Exception {

        db = TestDatabaseCreator.createTestDatabase();

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("setup took " + t + "ms"))) {
            gridBuilder = GraphBuilder.create(db)
                    .setLabel(LABEL)
                    .setRelationship(RELATIONSHIP)
                    .newGridBuilder()
                    .createGrid(10, 10, 1);
        }

        try (ProgressTimer timer = ProgressTimer.start(t -> System.out.println("load took " + t + "ms"))) {
            graph = new GraphLoader(db)
                    .withLabel(LABEL)
                    .withRelationshipType(RELATIONSHIP)
                    .withRelationshipWeightsFromProperty(PROPERTY, 1.0)
                    .load(HeavyGraphFactory.class);

            nodeCount = (int) graph.nodeCount();

            rootNodeId = graph.toMappedNodeId(gridBuilder.getLineNodes()
                    .get(0)
                    .get(0)
                    .getId());
        }
    }

    @AfterClass
    public static void tearDown() {
        db.shutdown();
    }


    @Test
    public void testTraverseLocal() throws Exception {

        final ParallelLocalQueueBFS traverse = new ParallelLocalQueueBFS(graph, Pools.DEFAULT, 10);

        traverse
                .reset()
                .bfs(rootNodeId, Direction.OUTGOING, n -> true, mock)
                .awaitTermination();

        System.out.println("traverse.getThreadsCreated() = " + traverse.getThreadsCreated());

        verify(mock, times(nodeCount)).accept(anyInt());

    }

    @Test
    public void testTraverseLocal2() throws Exception {

        final ParallelLocalQueueBFS traverse = new ParallelLocalQueueBFS(graph, Pools.DEFAULT, 10);
        final AtomicInteger ai = new AtomicInteger(0);

        for (int i = 0; i < 100; i++) {

            ai.set(0);

            traverse
                    .reset()
                    .withConcurrencyFactor(1)
                    .bfs(rootNodeId, Direction.OUTGOING, n -> true, node -> ai.incrementAndGet())
                    .awaitTermination();

            assertEquals("Iteration " + i + " results in error", nodeCount, ai.get());
        }

    }


}
