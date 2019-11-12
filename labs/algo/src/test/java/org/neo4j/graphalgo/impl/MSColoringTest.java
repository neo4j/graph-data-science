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

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.HugeGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MSColoringTest {

    private static final int NUM_SETS = 20;
    private static final int SET_SIZE = 1000;


    private static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    private GraphDatabaseAPI db;

    private Graph graph;

    @BeforeEach
    void setup() {
        db = TestDatabaseCreator.createTestDatabase();
        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("creating test graph took " + l + " ms"))) {
            createTestGraph();
        }
        graph = new GraphLoader(db)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(HugeGraphFactory.class);
    }

    @AfterEach
    void teardown() {
        db.shutdown();
    }

    private void createTestGraph() {
        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < NUM_SETS; i++) {
            runnables.add(createRing(RELATIONSHIP_TYPE));
        }
        ParallelUtil.run(runnables, Pools.DEFAULT);
    }

    private Runnable createRing(RelationshipType type) {
        return () -> {
            try (Transaction tx = db.beginTx()) {
                Node node = db.createNode();
                Node start = node;
                for (int i = 1; i < SET_SIZE; i++) {
                    Node temp = db.createNode();
                    node.createRelationshipTo(temp, type);
                    node = temp;
                }
                node.createRelationshipTo(start, type);
                tx.success();
            }
        };
    }

    @Test
    void testMsColoring() {
        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("MSColoring took " + l + "ms"))) {
            final AtomicIntegerArray colors = new MSColoring(graph, Pools.DEFAULT, 8)
                    .compute()
                    .getColors();

            assertEquals(NUM_SETS, numColors(colors));
        }
    }

    private static int numColors(AtomicIntegerArray colors) {
        final IntIntMap map = new IntIntScatterMap();
        for (int i = colors.length() - 1; i >= 0; i--) {
            map.addTo(colors.get(i), 1);
        }
        return map.size();
    }
}
