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
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraphFactory;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.internal.kernel.api.Write;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.internal.kernel.api.exceptions.schema.IllegalTokenNameException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicIntegerArray;

import static org.junit.Assert.assertEquals;

/**
 * @author mknblch
 */
public class MultiStepColoringTest {

    public static final int NUM_SETS = 20;
    public static final int SET_SIZE = 1000;


    public static final RelationshipType RELATIONSHIP_TYPE = RelationshipType.withName("TYPE");

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private Graph graph;

    @Before
    public void setup() throws Exception {
        try (ProgressTimer timer = ProgressTimer.start(l -> System.out.println("creating test graph took " + l + " ms"))) {
            createTestGraph();
        }

        graph = new GraphLoader(DB)
                .withExecutorService(Pools.DEFAULT)
                .withAnyLabel()
                .withRelationshipType(RELATIONSHIP_TYPE)
                .load(HeavyGraphFactory.class);
    }

    private static void createTestGraph() throws Exception {
        final int rIdx = DB.executeAndCommit((GraphDatabaseService __) -> {
            KernelTransaction transaction = DB.transaction();
            try {
                return transaction.tokenWrite().relationshipTypeGetOrCreateForName(RELATIONSHIP_TYPE.name());
            } catch (IllegalTokenNameException e) {
                throw new RuntimeException(e);
            }
        });

        final ArrayList<Runnable> runnables = new ArrayList<>();
        for (int i = 0; i < NUM_SETS; i++) {
            runnables.add(createRing(rIdx));
        }
        ParallelUtil.run(runnables, Pools.DEFAULT);
    }

    private static Runnable createRing(int rIdx) {
        return () -> {
            DB.executeAndCommit((GraphDatabaseService __) -> {
                try {
                    KernelTransaction transaction = DB.transaction();
                    final Write op = transaction.dataWrite();
                    long node = op.nodeCreate();
                    long start = node;
                    for (int i = 1; i < SET_SIZE; i++) {
                        final long temp = op.nodeCreate();
                        op.relationshipCreate(node, rIdx, temp);
                        node = temp;
                    }
                    op.relationshipCreate(node, rIdx, start);
                } catch (EntityNotFoundException | InvalidTransactionTypeKernelException e) {
                    throw new RuntimeException(e);
                }
            });
        };
    }

    @Test
    public void testMsColoring() {

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
