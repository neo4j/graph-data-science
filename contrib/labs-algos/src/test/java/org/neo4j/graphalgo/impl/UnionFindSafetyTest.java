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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.unionfind.UnionFindAlgo;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Exceptions;

import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.IntStream;

import static org.junit.Assert.assertSame;
import static org.neo4j.graphalgo.impl.unionfind.UnionFindAlgo.NOTHING;

@RunWith(Parameterized.class)
public final class UnionFindSafetyTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{UnionFindAlgo.QUEUE},
                new Object[]{UnionFindAlgo.FORK_JOIN},
                new Object[]{UnionFindAlgo.FJ_MERGE}
        );
    }

    @Parameterized.Parameter
    public UnionFindAlgo unionFindAlgo;

    @Test(timeout = 10_000L)
    public void testUnionFindSafetyUnderFailure() {
        IllegalStateException error = new IllegalStateException("some error");
        Graph graph = new FlakyGraph(100, 10, new Random(42L), error);
        try {
            unionFindAlgo.run(
                    graph,
                    Pools.DEFAULT,
                    AllocationTracker.EMPTY,
                    10,
                    10,
                    Double.NaN,
                    NOTHING
            );
        } catch (Throwable e) {
            assertSame(error, Exceptions.rootCause(e));
        }
    }

    @Test(timeout = 10_000L)
    public void testHugeUnionFindSafetyUnderFailure() {
        IllegalStateException error = new IllegalStateException("some error");
        Graph graph = new FlakyGraph(100, 10, new Random(42L), error);
        try {
            unionFindAlgo.run(
                    graph,
                    Pools.DEFAULT,
                    AllocationTracker.EMPTY,
                    10,
                    10,
                    Double.NaN,
                    NOTHING
            );
        } catch (Throwable e) {
            assertSame(error, Exceptions.rootCause(e));
        }
    }


    private static final class FlakyGraph implements Graph {
        private final int nodes;
        private final int maxDegree;
        private final Random random;
        private final RuntimeException error;

        private FlakyGraph(int nodes, int maxDegree, Random random, RuntimeException error) {
            this.nodes = nodes;
            this.maxDegree = maxDegree;
            this.random = random;
            this.error = error;
        }

        @Override
        public void canRelease(final boolean canRelease) {
        }

        @Override
        public long nodeCount() {
            return (long) nodes;
        }

        @Override
        public void forEachRelationship(
                final long nodeId,
                final Direction direction,
                final RelationshipConsumer consumer) {
            if (nodeId == 0L) {
                throw error;
            }
            int degree = random.nextInt(maxDegree);
            int[] targets = IntStream.range(0, degree)
                    .map(i -> random.nextInt(nodes))
                    .filter(i -> (long) i != nodeId)
                    .distinct()
                    .toArray();
            for (int target : targets) {
                if (!consumer.accept(nodeId, (long) target)) {
                    break;
                }
            }
        }

        @Override
        public RelationshipIntersect intersection() {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.intersection is not implemented.");
        }

        @Override
        public Collection<PrimitiveLongIterable> batchIterables(final int batchSize) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.hugeBatchIterables is not implemented.");
        }

        @Override
        public int degree(final long nodeId, final Direction direction) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.degree is not implemented.");
        }

        @Override
        public long toMappedNodeId(final long nodeId) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.toHugeMappedNodeId is not implemented.");
        }

        @Override
        public long toOriginalNodeId(final long nodeId) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.toOriginalNodeId is not implemented.");
        }

        @Override
        public boolean contains(final long nodeId) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.contains is not implemented.");
        }

        @Override
        public void forEachNode(final LongPredicate consumer) {

        }

        @Override
        public PrimitiveLongIterator nodeIterator() {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.hugeNodeIterator is not implemented.");
        }

        @Override
        public HugeWeightMapping nodeProperties(final String type) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.hugeNodeProperties is not implemented.");
        }

        @Override
        public long getTarget(final long nodeId, final long index, final Direction direction) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.getTarget is not implemented.");
        }

        @Override
        public void forEachRelationship(
                final long nodeId, final Direction direction, final WeightedRelationshipConsumer consumer) {

        }

        @Override
        public boolean exists(final long sourceNodeId, final long targetNodeId, final Direction direction) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.exists is not implemented.");
        }

        @Override
        public double weightOf(final long sourceNodeId, final long targetNodeId) {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.weightOf is not implemented.");
        }

        @Override
        public Set<String> availableNodeProperties() {
            throw new UnsupportedOperationException(
                    "org.neo4j.graphalgo.impl.UnionFindSafetyTest.FlakyGraph.availableNodeProperties is not implemented.");
        }
    }
}
