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
package org.neo4j.graphalgo.core.neo4jview;

import org.neo4j.collection.primitive.PrimitiveIntCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.IntIdMap;
import org.neo4j.graphalgo.core.huge.loader.NullWeightMap;
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
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongPredicate;
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
    private final Direction loadDirection;
    private final double propertyDefaultWeight;
    private final IntIdMap idMapping;
    private final boolean isUndirected;

    GraphView(
            GraphDatabaseAPI db,
            GraphDimensions dimensions,
            Direction loadDirection,
            IntIdMap idMapping,
            double propertyDefaultWeight,
            boolean isUndirected) {
        this.tx = new TransactionWrapper(db);
        this.dimensions = dimensions;
        this.loadDirection = loadDirection;
        this.propertyDefaultWeight = propertyDefaultWeight;
        this.idMapping = idMapping;
        this.isUndirected = isUndirected;
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        final WeightedRelationshipConsumer asWeighted =
                (sourceNodeId, targetNodeId, weight) ->
                        consumer.accept(sourceNodeId, targetNodeId);
        forAllRelationships(nodeId, direction, false, asWeighted);
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        forAllRelationships(nodeId, direction, true, consumer);
    }

    private void forAllRelationships(
            long nodeId,
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
                                weight = ReadHelper.readProperty(pc, dimensions.relProperties().weightId(), defaultWeight);
                            }
                        }
                        final long otherId = toMappedNodeId(cursor.otherNodeReference());
                        long relId = RawValues.combineIntInt(
                                (int) cursor.sourceNodeReference(),
                                (int) cursor.targetNodeReference());
                        if (!action.accept(nodeId, otherId,weight)) {
                            breaker.run();
                        }
                    };

                    LoadRelationships loader = rels(transaction);
                    if (direction == Direction.BOTH || (direction == Direction.OUTGOING && isUndirected) ) {
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
        return dimensions.nodeCount();
    }

    @Override
    public long relationshipCount() {
        return dimensions.maxRelCount();
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        idMapping.forEachNode(consumer);
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return idMapping.iterator();
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(final int batchSize) {
        int nodeCount = dimensions.nodeCountAsInt();
        int numberOfBatches = (int) Math.ceil(nodeCount / (double) batchSize);
        if (numberOfBatches == 1) {
            return Collections.singleton(this::nodeIterator);
        }
        PrimitiveLongIterable[] iterators = new PrimitiveLongIterable[numberOfBatches];
        Arrays.setAll(iterators, i -> () -> new SizedNodeIterator(nodeIterator(), i * batchSize, batchSize));
        return Arrays.asList(iterators);
    }

    @Override
    public int degree(long nodeId, Direction direction) {
        return withinTransactionInt(transaction -> {
            try (NodeCursor nc = transaction.cursors().allocateNodeCursor()) {
                transaction.dataRead().singleNode(toOriginalNodeId(nodeId), nc);
                if (nc.next()) {
                    LoadRelationships relationships = rels(transaction);
                    if (direction == Direction.BOTH || isUndirected && direction == Direction.OUTGOING) {
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
    public long toMappedNodeId(long nodeId) {
        return idMapping.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return idMapping.toOriginalNodeId(nodeId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return idMapping.contains(nodeId);
    }

    @Override
    public double weightOf(final long sourceNodeId, final long targetNodeId) {
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
                                double weight = ReadHelper.readProperty(pc, dimensions.relProperties().weightId(), defaultWeight);
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

    @Override
    public WeightMapping nodeProperties(String type) {
        return new NullWeightMap(1D);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return Collections.emptySet();
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
        return LoadRelationships.of(transaction.cursors(), dimensions.relationshipTypeMappings().relationshipTypeIds());
    }

    @Override
    public long getTarget(long nodeId, long index, Direction direction) {
        GetTargetConsumer consumer = new GetTargetConsumer(index);
        forEachRelationship(nodeId, direction, consumer);
        return consumer.found;
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        ExistsConsumer existsConsumer = new ExistsConsumer(targetNodeId);
        forEachRelationship(sourceNodeId, direction, existsConsumer);
        return existsConsumer.found;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public boolean isUndirected() {
        return isUndirected;
    }

    @Override
    public Direction getLoadDirection() {
        return loadDirection;
    }

    @Override
    public void canRelease(boolean canRelease) {
    }

    @Override
    public RelationshipIntersect intersection() {
        throw new UnsupportedOperationException("Not implemented for Graph View");
    }

    private static class SizedNodeIterator implements PrimitiveLongIterator {

        private final PrimitiveLongIterator iterator;
        private int remaining;

        private SizedNodeIterator(
                PrimitiveLongIterator iterator,
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
        public long next() {
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
        private final long index;
        long count;
        long found;

        public GetTargetConsumer(long index) {
            this.index = index;
            count = index;
            found = -1;
        }

        @Override
        public boolean accept(long s, long t) {
            if (count-- == 0) {
                found = t;
                return false;
            }
            return true;
        }
    }

    private static class ExistsConsumer implements RelationshipConsumer {
        private final long targetNodeId;
        private boolean found = false;

        public ExistsConsumer(long targetNodeId) {
            this.targetNodeId = targetNodeId;
        }

        @Override
        public boolean accept(long s, long t) {
            if (t == targetNodeId) {
                found = true;
                return false;
            }
            return true;
        }
    }
}
