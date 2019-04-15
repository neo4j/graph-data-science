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
package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.IdMap;
import org.neo4j.graphalgo.core.loading.LoadRelationships;
import org.neo4j.graphalgo.core.loading.ReadHelper;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphalgo.core.utils.TransactionWrapper;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.helpers.RelationshipSelectionCursor;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;

/**
 * A Graph implemented as View on Neo4j Kernel API
 *
 * @author mknobloch
 */
public class GraphView implements Graph {

    public static final String TYPE = "kernel";

    private final TransactionWrapper tx;

    private final GraphDimensions dimensions;
    private final double propertyDefaultWeight;
    private final IdMap idMapping;
    private boolean loadAsUndirected;

    GraphView(
            GraphDatabaseAPI db,
            GraphDimensions dimensions,
            IdMap idMapping,
            double propertyDefaultWeight, boolean loadAsUndirected) {
        this.tx = new TransactionWrapper(db);
        this.dimensions = dimensions;
        this.propertyDefaultWeight = propertyDefaultWeight;
        this.idMapping = idMapping;
        this.loadAsUndirected = loadAsUndirected;
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer) {
        final WeightedRelationshipConsumer asWeighted =
                (sourceNodeId, targetNodeId, relationId, weight) ->
                        consumer.accept(sourceNodeId, targetNodeId, relationId);
        forAllRelationships(nodeId, direction, false, asWeighted);
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        forAllRelationships(nodeId, direction, true, consumer);
    }

    private void forAllRelationships(
            int nodeId,
            Direction direction,
            boolean readWeights,
            WeightedRelationshipConsumer action) {
        final long originalNodeId = toOriginalNodeId(nodeId);
        withBreaker(breaker -> {
            withinTransaction(transaction -> {
                CursorFactory cursors = transaction.cursors();
                Read read = transaction.dataRead();
                try (NodeCursor nc = cursors.allocateNodeCursor();
                     RelationshipScanCursor rc = cursors.allocateRelationshipScanCursor();
                     PropertyCursor pc = cursors.allocatePropertyCursor()) {

                    read.singleNode(originalNodeId, nc);
                    if (!nc.next()) {
                        breaker.run();
                    }

                    final double defaultWeight = this.propertyDefaultWeight;
                    Consumer<RelationshipSelectionCursor> visitor = (cursor) -> {
                        if (!idMapping.contains(cursor.otherNodeReference())) {
                            return;
                        }
                        double weight = defaultWeight;
                        if (readWeights) {
                            read.singleRelationship(cursor.relationshipReference(), rc);
                            if (rc.next()) {
                                rc.properties(pc);
                                weight = ReadHelper.readProperty(pc, dimensions.relWeightId(), defaultWeight);
                            }
                        }
                        final int otherId = toMappedNodeId(cursor.otherNodeReference());
                        long relId = RawValues.combineIntInt(
                                (int) cursor.sourceNodeReference(),
                                (int) cursor.targetNodeReference());
                        if (!action.accept(nodeId, otherId, relId, weight)) {
                            breaker.run();
                        }
                    };

                    LoadRelationships loader = rels(transaction);
                    if (direction == Direction.BOTH || (direction == Direction.OUTGOING && loadAsUndirected) ) {
                        // can't use relationshipsBoth here, b/c we want to be consistent with the other graph impls
                        // that are iteration first over outgoing, then over incoming relationships
                        RelationshipSelectionCursor cursor = loader.relationshipsOut(nc);
                        LoadRelationships.consumeRelationships(cursor, visitor);
                        cursor = loader.relationshipsIn(nc);
                        LoadRelationships.consumeRelationships(cursor, visitor);
                    } else {
                        RelationshipSelectionCursor cursor = loader.relationshipsOf(direction, nc);
                        LoadRelationships.consumeRelationships(cursor, visitor);
                    }
                }
            });
        });
    }

    @Override
    public long nodeCount() {
        return dimensions.hugeNodeCount();
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        idMapping.forEachNode(consumer);
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        return idMapping.nodeIterator();
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(final int batchSize) {
        int nodeCount = dimensions.nodeCount();
        int numberOfBatches = (int) Math.ceil(nodeCount / (double) batchSize);
        if (numberOfBatches == 1) {
            return Collections.singleton(this::nodeIterator);
        }
        PrimitiveIntIterable[] iterators = new PrimitiveIntIterable[numberOfBatches];
        Arrays.setAll(iterators, i -> () -> new SizedNodeIterator(nodeIterator(), i * batchSize, batchSize));
        return Arrays.asList(iterators);
    }

    @Override
    public int degree(int nodeId, Direction direction) {
        return withinTransactionInt(transaction -> {
            try (NodeCursor nc = transaction.cursors().allocateNodeCursor()) {
                transaction.dataRead().singleNode(toOriginalNodeId(nodeId), nc);
                if (nc.next()) {
                    LoadRelationships relationships = rels(transaction);
                    if (direction == Direction.BOTH || loadAsUndirected && direction == Direction.OUTGOING) {
                        return relationships.degreeBoth(nc);
                    }
                    return direction == Direction.OUTGOING ?
                            relationships.degreeOut(nc) :
                            relationships.degreeIn(nc);
                }
                return 0;
            }
        });
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        return idMapping.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        return idMapping.toOriginalNodeId(nodeId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return idMapping.contains(nodeId);
    }

    @Override
    public double weightOf(final int sourceNodeId, final int targetNodeId) {
        final long sourceId = toOriginalNodeId(sourceNodeId);
        final long targetId = toOriginalNodeId(targetNodeId);

        return withinTransactionDouble(transaction -> {
            final double defaultWeight = this.propertyDefaultWeight;
            final double[] nodeWeight = {defaultWeight};
            withBreaker(breaker -> {
                CursorFactory cursors = transaction.cursors();
                Read read = transaction.dataRead();
                try (NodeCursor nc = cursors.allocateNodeCursor();
                     RelationshipScanCursor rc = cursors.allocateRelationshipScanCursor();
                     PropertyCursor pc = cursors.allocatePropertyCursor()) {

                    read.singleNode(sourceId, nc);
                    if (!nc.next()) {
                        breaker.run();
                    }

                    Consumer<RelationshipSelectionCursor> visitor = (cursor) -> {
                        if (targetId == cursor.otherNodeReference()) {
                            read.singleRelationship(cursor.relationshipReference(), rc);
                            if (rc.next()) {
                                rc.properties(pc);
                                double weight = ReadHelper.readProperty(pc, dimensions.relWeightId(), defaultWeight);
                                if (weight != defaultWeight) {
                                    nodeWeight[0] = weight;
                                    breaker.run();
                                }
                            }
                        }
                    };

                    LoadRelationships loader = rels(transaction);
                    RelationshipSelectionCursor cursor = loader.relationshipsOut(nc);
                    LoadRelationships.consumeRelationships(cursor, visitor);
                }
            });
            return nodeWeight[0];
        });
    }

    private int withinTransactionInt(ToIntFunction<KernelTransaction> block) {
        return tx.applyAsInt(block);
    }

    private double withinTransactionDouble(ToDoubleFunction<KernelTransaction> block) {
        return tx.applyAsDouble(block);
    }

    private void withinTransaction(Consumer<KernelTransaction> block) {
        tx.accept(block);
    }

    private LoadRelationships rels(KernelTransaction transaction) {
        return LoadRelationships.of(transaction.cursors(), dimensions.relationshipTypeId());
    }

    @Override
    public int getTarget(int nodeId, int index, Direction direction) {
        GetTargetConsumer consumer = new GetTargetConsumer(index);
        forEachRelationship(nodeId, direction, consumer);
        return consumer.found;
    }

    @Override
    public boolean exists(int sourceNodeId, int targetNodeId, Direction direction) {
        ExistsConsumer existsConsumer = new ExistsConsumer(targetNodeId);
        forEachRelationship(sourceNodeId, direction, existsConsumer);
        return existsConsumer.found;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void canRelease(boolean canRelease) {
    }

    @Override
    public RelationshipIntersect intersection() {
        throw new UnsupportedOperationException("Not implemented for Graph View");
    }

    private static class SizedNodeIterator implements PrimitiveIntIterator {

        private final PrimitiveIntIterator iterator;
        private int remaining;

        private SizedNodeIterator(
                PrimitiveIntIterator iterator,
                int start,
                int length) {
            while (iterator.hasNext() && start-- > 0) {
                iterator.next();
            }
            this.iterator = iterator;
            this.remaining = length;
        }

        @Override
        public boolean hasNext() {
            return remaining > 0 && iterator.hasNext();
        }

        @Override
        public int next() {
            remaining--;
            return iterator.next();
        }
    }

    private static class PrimitiveIntRangeIterator extends PrimitiveIntCollections.PrimitiveIntBaseIterator {
        private int current;
        private final int end;

        PrimitiveIntRangeIterator(int start, int end) {
            this.current = start;
            this.end = end;
        }

        @Override
        protected boolean fetchNext() {
            try {
                return current <= end && next(current);
            } finally {
                current++;
            }
        }
    }

    private static void withBreaker(Consumer<Runnable> block) {
        try {
            block.accept(BreakIteration.BREAK);
        } catch (BreakIteration ignore) {
        }
    }

    private static final class BreakIteration extends RuntimeException implements Runnable {

        private static final BreakIteration BREAK = new BreakIteration();

        BreakIteration() {
            super(null, null, false, false);
        }

        @Override
        public void run() {
            throw this;
        }
    }

    private static class GetTargetConsumer implements RelationshipConsumer {
        private final int index;
        int count;
        int found;

        public GetTargetConsumer(int index) {
            this.index = index;
            count = index;
            found = -1;
        }

        @Override
        public boolean accept(int s, int t, long r) {
            if (count-- == 0) {
                found = t;
                return false;
            }
            return true;
        }
    }

    private static class ExistsConsumer implements RelationshipConsumer {
        private final int targetNodeId;
        private boolean found = false;

        public ExistsConsumer(int targetNodeId) {
            this.targetNodeId = targetNodeId;
        }

        @Override
        public boolean accept(int s, int t, long r) {
            if (t == targetNodeId) {
                found = true;
                return false;
            }
            return true;
        }
    }
}
