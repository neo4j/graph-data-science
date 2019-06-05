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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.huge.loader.IdMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.LongPredicate;

/**
 * Huge Graph contains two array like data structures.
 * <p>
 * The adjacency data is stored in a ByteArray, which is a byte[] addressable by
 * longs indices and capable of storing about 2^46 (~ 70k bn) bytes – or 64 TiB.
 * The bytes are stored in byte[] pages of 32 KiB size.
 * <p>
 * The data is in the format:
 * <blockquote>
 * <code>degree</code> ~ <code>targetId</code><sub><code>1</code></sub> ~ <code>targetId</code><sub><code>2</code></sub> ~ <code>targetId</code><sub><code>n</code></sub>
 * </blockquote>
 * The {@code degree} is stored as a fill-sized 4 byte long {@code int}
 * (the neo kernel api returns an int for {@link org.neo4j.internal.kernel.api.helpers.Nodes#countAll(NodeCursor, CursorFactory)}).
 * Every target ID is first sorted, then delta encoded, and finally written as variable-length vlongs.
 * The delta encoding does not write the actual value but only the difference to the previous value, which plays very nice with the vlong encoding.
 * <p>
 * The seconds data structure is a LongArray, which is a long[] addressable by longs
 * and capable of storing about 2^43 (~9k bn) longs – or 64 TiB worth of 64 bit longs.
 * The data is the offset address into the aforementioned adjacency array, the index is the respective source node id.
 * <p>
 * To traverse all nodes, first access to offset from the LongArray, then read
 * 4 bytes into the {@code degree} from the ByteArray, starting from the offset, then read
 * {@code degree} vlongs as targetId.
 * <p>
 * <p>
 * The graph encoding (sans delta+vlong) is similar to that of the
 * {@link org.neo4j.graphalgo.core.lightweight.LightGraph} but stores degree
 * explicitly into the target adjacency array where the LightGraph would subtract
 * offsets of two consecutive nodes. While that doesn't use up memory to store the
 * degree, it makes it practically impossible to build the array out-of-order,
 * which is necessary for loading the graph in parallel.
 * <p>
 * Reading the degree from the offset position not only does not require the offset array
 * to be sorted but also allows the adjacency array to be sparse. This fact is
 * used during the import – each thread pre-allocates a local chunk of some pages (512 KiB)
 * and gives access to this data during import. Synchronization between threads only
 * has to happen when a new chunk has to be pre-allocated. This is similar to
 * what most garbage collectors do with TLAB allocations.
 *
 * @see <a href="https://developers.google.com/protocol-buffers/docs/encoding#varints">more abount vlong</a>
 * @see <a href="https://shipilev.net/jvm-anatomy-park/4-tlab-allocation/">more abount TLAB allocation</a>
 */
public class HugeGraph implements Graph {

    private final IdMap idMapping;
    private final AllocationTracker tracker;

    private HugeWeightMapping weights;
    private Map<String, HugeWeightMapping> nodeProperties;
    private final long relationshipCount;
    private HugeAdjacencyList inAdjacency;
    private HugeAdjacencyList outAdjacency;
    private HugeAdjacencyOffsets inOffsets;
    private HugeAdjacencyOffsets outOffsets;
    private HugeAdjacencyList.Cursor empty;
    private HugeAdjacencyList.Cursor inCache;
    private HugeAdjacencyList.Cursor outCache;
    private boolean canRelease = true;

    public HugeGraph(
            final AllocationTracker tracker,
            final IdMap idMapping,
            final HugeWeightMapping weights,
            final Map<String, HugeWeightMapping> nodeProperties,
            final long relationshipCount,
            final HugeAdjacencyList inAdjacency,
            final HugeAdjacencyList outAdjacency,
            final HugeAdjacencyOffsets inOffsets,
            final HugeAdjacencyOffsets outOffsets) {
        this.idMapping = idMapping;
        this.tracker = tracker;
        this.weights = weights;
        this.nodeProperties = nodeProperties;
        this.relationshipCount = relationshipCount;
        this.inAdjacency = inAdjacency;
        this.outAdjacency = outAdjacency;
        this.inOffsets = inOffsets;
        this.outOffsets = outOffsets;
        inCache = newCursor(this.inAdjacency);
        outCache = newCursor(this.outAdjacency);
        empty = inCache == null ? newCursor(this.outAdjacency) : newCursor(this.inAdjacency);
    }

    @Override
    public long nodeCount() {
        return idMapping.nodeCount();
    }

    @Override
    public long relationshipCount() {
        return relationshipCount;
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(final int batchSize) {
        return idMapping.batchIterables(batchSize);
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        idMapping.forEachNode(consumer);
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return idMapping.nodeIterator();
    }

    @Override
    public double weightOf(final long sourceNodeId, final long targetNodeId) {
        return weights.weight(sourceNodeId, targetNodeId);
    }

    @Override
    public HugeWeightMapping nodeProperties(final String type) {
        return nodeProperties.get(type);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return nodeProperties.keySet();
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        runForEach(nodeId, direction, consumer, /* reuseCursor */ true);
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        switch (direction) {
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                return;

            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                return;

            default:
                forEachOutgoing(nodeId, consumer);
                forEachIncoming(nodeId, consumer);
        }
    }

    @Override
    public int degree(
            final long node,
            final Direction direction) {
        switch (direction) {
            case INCOMING:
                return degree(node, inOffsets, inAdjacency);

            case OUTGOING:
                return degree(node, outOffsets, outAdjacency);

            case BOTH:
                return degree(node, inOffsets, inAdjacency) + degree(
                        node,
                        outOffsets,
                        outAdjacency);

            default:
                throw new IllegalArgumentException(direction + "");
        }
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
    public void forEachIncoming(long node, final RelationshipConsumer consumer) {
        runForEach(node, Direction.INCOMING, consumer, /* reuseCursor */ true);
    }

    public void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        runForEach(
                Integer.toUnsignedLong(nodeId),
                Direction.INCOMING,
                toHugeInConsumer(consumer),
                /* reuseCursor */ false
        );
    }

    public void forEachIncoming(long nodeId, WeightedRelationshipConsumer consumer) {
        runForEach(
                nodeId,
                Direction.INCOMING,
                toHugeInConsumer(consumer),
                /* reuseCursor */ false
        );
    }

    @Override
    public void forEachOutgoing(long node, final RelationshipConsumer consumer) {
        runForEach(node, Direction.OUTGOING, consumer, /* reuseCursor */ true);
    }

//    @Override
//    public void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
//        runForEach(
//                Integer.toUnsignedLong(nodeId),
//                Direction.OUTGOING,
//                toHugeOutConsumer(consumer),
//                /* reuseCursor */ false
//        );
//    }

    public void forEachOutgoing(long nodeId, WeightedRelationshipConsumer consumer) {
        runForEach(
                nodeId,
                Direction.OUTGOING,
                toHugeOutConsumer(consumer),
                /* reuseCursor */ false
        );
    }

    @Override
    public Graph concurrentCopy() {
        return new HugeGraph(
                tracker,
                idMapping,
                weights,
                nodeProperties,
                relationshipCount, inAdjacency,
                outAdjacency,
                inOffsets,
                outOffsets
        );
    }

    @Override
    public RelationshipIntersect intersection() {
        return new HugeGraphIntersectImpl(outAdjacency, outOffsets);
    }

//    /**
//     * O(n) !
//     */
//    @Override
//    public boolean exists(int sourceNodeId, int targetNodeId, Direction direction) {
//        return exists(
//                Integer.toUnsignedLong(sourceNodeId),
//                Integer.toUnsignedLong(targetNodeId),
//                direction,
//                // Graph interface should be thread-safe
//                false
//        );
//    }

    /**
     * O(n) !
     */
    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        return exists(
                sourceNodeId,
                targetNodeId,
                direction,
                // HugeGraph interface make no promises about thread-safety (that's what concurrentCopy is for)
                true
        );
    }

    private boolean exists(long sourceNodeId, long targetNodeId, Direction direction, boolean reuseCursor) {
        ExistsConsumer consumer = new ExistsConsumer(targetNodeId);
        runForEach(sourceNodeId, direction, consumer, reuseCursor);
        return consumer.found;
    }

//    /**
//     * O(n) !
//     */
//    @Override
//    public int getTarget(int nodeId, int index, Direction direction) {
//        return Math.toIntExact(getTarget(
//                Integer.toUnsignedLong(nodeId),
//                Integer.toUnsignedLong(index),
//                direction,
//                // Graph interface should be thread-safe
//                false
//        ));
//    }

    /*
     * O(n) !
     */
    @Override
    public long getTarget(long sourceNodeId, long index, Direction direction) {
        return getTarget(
                sourceNodeId,
                index,
                direction,
                // HugeGraph interface make no promises about thread-safety (that's what concurrentCopy is for)
                true
        );
    }

    private long getTarget(long sourceNodeId, long index, Direction direction, boolean reuseCursor) {
        GetTargetConsumer consumer = new GetTargetConsumer(index);
        runForEach(sourceNodeId, direction, consumer, reuseCursor);
        return consumer.target;
    }

    private void runForEach(
            long sourceNodeId,
            Direction direction,
            RelationshipConsumer consumer,
            boolean reuseCursor) {
        if (direction == Direction.BOTH) {
            runForEach(sourceNodeId, Direction.OUTGOING, consumer, reuseCursor);
            runForEach(sourceNodeId, Direction.INCOMING, consumer, reuseCursor);
            return;
        }
        HugeAdjacencyList.Cursor cursor = forEachCursor(sourceNodeId, direction, reuseCursor);
        consumeNodes(sourceNodeId, cursor, consumer);
    }

    private HugeAdjacencyList.Cursor forEachCursor(
            long sourceNodeId,
            Direction direction,
            boolean reuseCursor) {
        if (direction == Direction.OUTGOING) {
            return cursor(
                    sourceNodeId,
                    reuseCursor ? outCache : outAdjacency.newCursor(),
                    outOffsets,
                    outAdjacency);
        } else {
            return cursor(
                    sourceNodeId,
                    reuseCursor ? inCache : inAdjacency.newCursor(),
                    inOffsets,
                    inAdjacency);
        }
    }

    @Override
    public void canRelease(boolean canRelease) {
        this.canRelease = canRelease;
    }

    @Override
    public void release() {
        if (!canRelease) return;
        if (inAdjacency != null) {
            tracker.remove(inAdjacency.release());
            tracker.remove(inOffsets.release());
            inAdjacency = null;
            inOffsets = null;
        }
        if (outAdjacency != null) {
            tracker.remove(outAdjacency.release());
            tracker.remove(outOffsets.release());
            outAdjacency = null;
            outOffsets = null;
        }
        if (weights != null) {
            tracker.remove(weights.release());
        }
        for (final HugeWeightMapping nodeMapping : nodeProperties.values()) {
            tracker.remove(nodeMapping.release());
        }
        empty = null;
        inCache = null;
        outCache = null;
        weights = null;
    }

    private HugeAdjacencyList.Cursor newCursor(final HugeAdjacencyList adjacency) {
        return adjacency != null ? adjacency.newCursor() : null;
    }

    private int degree(long node, HugeAdjacencyOffsets offsets, HugeAdjacencyList array) {
        long offset = offsets.get(node);
        if (offset == 0L) {
            return 0;
        }
        return array.getDegree(offset);
    }

    private HugeAdjacencyList.Cursor cursor(
            long node,
            HugeAdjacencyList.Cursor reuse,
            HugeAdjacencyOffsets offsets,
            HugeAdjacencyList array) {
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return empty;
        }
        return array.deltaCursor(reuse, offset);
    }

    private void consumeNodes(
            long startNode,
            HugeAdjacencyList.Cursor cursor,
            RelationshipConsumer consumer) {
        //noinspection StatementWithEmptyBody
        while (cursor.hasNextVLong() && consumer.accept(startNode, cursor.nextVLong())) ;
    }

    private RelationshipConsumer toHugeOutConsumer(RelationshipConsumer consumer) {
        return (s, t) -> consumer.accept(
                (int) s,
                (int) t);
    }

    private RelationshipConsumer toHugeInConsumer(RelationshipConsumer consumer) {
        return (s, t) -> consumer.accept(
                (int) s,
                (int) t);
    }

    private RelationshipConsumer toHugeOutConsumer(WeightedRelationshipConsumer consumer) {
        return (s, t) -> {
            double weight = weightOf(s, t);
            return consumer.accept(
                    (int) s,
                    (int) t,
                    weight);
        };
    }

    private RelationshipConsumer toHugeInConsumer(WeightedRelationshipConsumer consumer) {
        return (s, t) -> {
            double weight = weightOf(t, s);
            return consumer.accept(
                    (int) s,
                    (int) t,
                    weight);
        };
    }

    private static class GetTargetConsumer implements RelationshipConsumer {
        private long count;
        private long target = -1;

        public GetTargetConsumer(long count) {
            this.count = count;
        }

        @Override
        public boolean accept(long s, long t) {
            if (count-- == 0) {
                target = t;
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
