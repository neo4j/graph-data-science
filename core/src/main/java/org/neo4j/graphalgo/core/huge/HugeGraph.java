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
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
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

    public static final double NO_WEIGHT = Double.NaN;
    public static final int NO_SUCH_NODE = 0;

    private final IdMap idMapping;
    private final AllocationTracker tracker;

    private final Map<String, WeightMapping> nodeProperties;
    private final long relationshipCount;
    private AdjacencyList inAdjacency;
    private AdjacencyList outAdjacency;
    private AdjacencyOffsets inOffsets;
    private AdjacencyOffsets outOffsets;

    private final double defaultWeight;
    private AdjacencyList inWeights;
    private AdjacencyList outWeights;
    private AdjacencyOffsets inWeightOffsets;
    private AdjacencyOffsets outWeightOffsets;

    private AdjacencyList.DecompressingCursor emptyAdjacencyCursor;
    private AdjacencyList.DecompressingCursor inCache;
    private AdjacencyList.DecompressingCursor outCache;

    private boolean canRelease = true;

    private final boolean hasRelationshipProperty;
    private final boolean isUndirected;


    public static HugeGraph create(
        final AllocationTracker tracker,
        final IdMap idMapping,
        final Map<String, WeightMapping> nodeProperties,
        final long relationshipCount,
        final AdjacencyList inAdjacency,
        final AdjacencyList outAdjacency,
        final AdjacencyOffsets inOffsets,
        final AdjacencyOffsets outOffsets,
        final Optional<Double> defaultWeight,
        final Optional<AdjacencyList> inWeights,
        final Optional<AdjacencyList> outWeights,
        final Optional<AdjacencyOffsets> inWeightOffsets,
        final Optional<AdjacencyOffsets> outWeightOffsets,
        final boolean isUndirected
    ) {
        return new HugeGraph(
            tracker,
            idMapping,
            nodeProperties,
            relationshipCount,
            inAdjacency,
            outAdjacency,
            inOffsets,
            outOffsets,
            inWeights.isPresent() || outWeights.isPresent(),
            defaultWeight.orElse(Double.NaN),
            inWeights.orElse(null),
            outWeights.orElse(null),
            inWeightOffsets.orElse(null),
            outWeightOffsets.orElse(null),
            isUndirected
        );
    }


    public HugeGraph(
        final AllocationTracker tracker,
        final IdMap idMapping,
        final Map<String, WeightMapping> nodeProperties,
        final long relationshipCount,
        final AdjacencyList inAdjacency,
        final AdjacencyList outAdjacency,
        final AdjacencyOffsets inOffsets,
        final AdjacencyOffsets outOffsets,
        final boolean hasRelationshipProperty,
        final double defaultWeight,
        final AdjacencyList inWeights,
        final AdjacencyList outWeights,
        final AdjacencyOffsets inWeightOffsets,
        final AdjacencyOffsets outWeightOffsets,
        final boolean isUndirected
    ) {
        this.idMapping = idMapping;
        this.tracker = tracker;
        this.nodeProperties = nodeProperties;
        this.relationshipCount = relationshipCount;
        this.inAdjacency = inAdjacency;
        this.outAdjacency = outAdjacency;
        this.inOffsets = inOffsets;
        this.outOffsets = outOffsets;
        this.defaultWeight = defaultWeight;
        this.inWeights = inWeights;
        this.outWeights = outWeights;
        this.inWeightOffsets = inWeightOffsets;
        this.outWeightOffsets = outWeightOffsets;
        this.isUndirected = isUndirected;
        this.hasRelationshipProperty = hasRelationshipProperty;
        inCache = newAdjacencyCursor(this.inAdjacency);
        outCache = newAdjacencyCursor(this.outAdjacency);
        emptyAdjacencyCursor = inCache == null ? newAdjacencyCursor(this.outAdjacency) : newAdjacencyCursor(this.inAdjacency);
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
    public double weightOf(final long sourceNodeId, final long targetNodeId, double fallbackValue) {
        if (!hasRelationshipProperty) {
            return fallbackValue;
        }

        double maybeWeight;

        if (outWeights != null) {
            maybeWeight = findWeight(
                sourceNodeId,
                targetNodeId,
                outWeights,
                outWeightOffsets,
                outAdjacency,
                outOffsets);
            if (!Double.isNaN(maybeWeight)) {
                return maybeWeight;
            }
        }

        if (inWeights != null) {
            maybeWeight = findWeight(targetNodeId, sourceNodeId, inWeights, inWeightOffsets, inAdjacency, inOffsets);

            if (!Double.isNaN(maybeWeight)) {
                return maybeWeight;
            }
        }

        return defaultWeight;
    }

    private double findWeight(
        final long fromId,
        final long toId,
        final AdjacencyList weights,
        final AdjacencyOffsets weightOffsets,
        final AdjacencyList adjacencies,
        final AdjacencyOffsets adjacencyOffsets
    ) {
        long relOffset = adjacencyOffsets.get(fromId);
        if (relOffset == NO_SUCH_NODE) {
            return NO_WEIGHT;
        }
        long weightOffset = weightOffsets.get(fromId);

        AdjacencyList.DecompressingCursor relDecompressingCursor = adjacencies.decompressingCursor(relOffset);
        AdjacencyList.Cursor weightCursor = weights.cursor(weightOffset);

        while (relDecompressingCursor.hasNextVLong() && weightCursor.hasNextLong() && relDecompressingCursor.nextVLong() != toId) {
            weightCursor.nextLong();
        }

        if (!weightCursor.hasNextLong()) {
            return NO_WEIGHT;
        }

        long doubleBits = weightCursor.nextLong();
        return Double.longBitsToDouble(doubleBits);
    }

    @Override
    public WeightMapping nodeProperties(final String type) {
        return nodeProperties.get(type);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return nodeProperties.keySet();
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        switch (direction) {
            case INCOMING:
                runForEach(nodeId, Direction.INCOMING, consumer, /* reuseCursor */ true);
                return;

            case OUTGOING:
                runForEach(nodeId, Direction.OUTGOING, consumer, /* reuseCursor */ true);
                return;

            default:
                runForEach(nodeId, Direction.OUTGOING, consumer, /* reuseCursor */ true);
                runForEach(nodeId, Direction.INCOMING, consumer, /* reuseCursor */ true);
        }
    }

    @Override
    public void forEachRelationship(
        long nodeId,
        Direction direction,
        double fallbackValue,
        WeightedRelationshipConsumer consumer
    ) {

        switch (direction) {
            case INCOMING:
                runForEachWithWeight(nodeId, Direction.INCOMING, fallbackValue, consumer);
                return;

            case OUTGOING:
                runForEachWithWeight(nodeId, Direction.OUTGOING, fallbackValue, consumer);
                return;

            default:
                runForEachWithWeight(nodeId, Direction.OUTGOING, fallbackValue, consumer);
                runForEachWithWeight(nodeId, Direction.INCOMING, fallbackValue, consumer);
        }
    }

    @Override
    public int degree(
        final long node,
        final Direction direction
    ) {
        switch (direction) {
            case INCOMING:
                return degree(node, inOffsets, inAdjacency);

            case OUTGOING:
                return degree(node, outOffsets, outAdjacency);

            case BOTH:
                return degree(node, inOffsets, inAdjacency) +
                       degree(node, outOffsets, outAdjacency);

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

    @Override
    public void forEachOutgoing(long node, final RelationshipConsumer consumer) {
        runForEach(node, Direction.OUTGOING, consumer, /* reuseCursor */ true);
    }

    @Override
    public HugeGraph concurrentCopy() {
        return new HugeGraph(
            tracker,
            idMapping,
            nodeProperties,
            relationshipCount,
            inAdjacency,
            outAdjacency,
            inOffsets,
            outOffsets,
            hasRelationshipProperty,
            defaultWeight,
            inWeights,
            outWeights,
            inWeightOffsets,
            outWeightOffsets,
            isUndirected);
    }

    @Override
    public RelationshipIntersect intersection() {
        return new HugeGraphIntersectImpl(outAdjacency, outOffsets);
    }

    /**
     * O(n) !
     */
    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        ExistsConsumer consumer = new ExistsConsumer(targetNodeId);
        runForEach(sourceNodeId, direction, consumer,
            // HugeGraph interface make no promises about thread-safety (that's what concurrentCopy is for)
            false);
        return consumer.found;
    }

    /*
     * O(n) !
     */
    @Override
    public long getTarget(long sourceNodeId, long index, Direction direction) {
        GetTargetConsumer consumer = new GetTargetConsumer(index);
        runForEach(sourceNodeId, direction, consumer,
            // HugeGraph interface make no promises about thread-safety (that's what concurrentCopy is for)
            false);
        return consumer.target;
    }

    private void runForEach(
        long sourceNodeId,
        Direction direction,
        RelationshipConsumer consumer,
        boolean reuseCursor
    ) {

        if (direction == Direction.BOTH) {
            runForEach(sourceNodeId, Direction.OUTGOING, consumer, reuseCursor);
            runForEach(sourceNodeId, Direction.INCOMING, consumer, reuseCursor);
            return;
        }

        AdjacencyList.DecompressingCursor adjacencyCursor = adjacencyCursorForIteration(
            sourceNodeId,
            direction,
            reuseCursor);
        consumeAdjacentNodes(sourceNodeId, adjacencyCursor, consumer);
    }

    private void runForEachWithWeight(
        long sourceNodeId,
        Direction direction,
        double fallbackValue,
        WeightedRelationshipConsumer consumer
    ) {

        if (direction == Direction.BOTH) {
            runForEachWithWeight(sourceNodeId, Direction.OUTGOING, fallbackValue, consumer);
            runForEachWithWeight(sourceNodeId, Direction.INCOMING, fallbackValue, consumer);
            return;
        }

        if (!hasRelationshipProperty()) {
            runForEach(sourceNodeId, direction, (s, t) -> consumer.accept(s, t, fallbackValue), false);
        } else {
            AdjacencyList.DecompressingCursor adjacencyCursor = adjacencyCursorForIteration(
                sourceNodeId,
                direction,
                false
            );

            AdjacencyList.Cursor weightCursor = weightCursorForIteration(sourceNodeId, direction);
            consumeAdjacentNodesWithWeight(sourceNodeId, adjacencyCursor, weightCursor, consumer);
        }
    }

    private AdjacencyList.DecompressingCursor adjacencyCursorForIteration(
        long sourceNodeId,
        Direction direction,
        boolean reuseCursor
    ) {
        if (direction == Direction.OUTGOING) {
            return adjacencyCursor(
                sourceNodeId,
                reuseCursor ? outCache : outAdjacency.rawDecompressingCursor(),
                outOffsets,
                outAdjacency);
        } else {
            return adjacencyCursor(
                sourceNodeId,
                reuseCursor ? inCache : inAdjacency.rawDecompressingCursor(),
                inOffsets,
                inAdjacency);
        }
    }

    private AdjacencyList.Cursor weightCursorForIteration(long sourceNodeId, Direction direction) {
        if (direction == Direction.OUTGOING) {
            return weightCursor(
                sourceNodeId,
                outWeightOffsets,
                outWeights);
        } else {
            return weightCursor(
                sourceNodeId,
                inWeightOffsets,
                inWeights);
        }
    }

    @Override
    public void canRelease(boolean canRelease) {
        this.canRelease = canRelease;
    }

    @Override
    public void releaseTopology() {
        if (!canRelease) return;
        if (inAdjacency != null) {
            tracker.remove(inAdjacency.release());
            tracker.remove(inOffsets.release());
            inAdjacency = null;
            inWeights = null;
            inOffsets = null;
            inWeightOffsets = null;
        }
        if (outAdjacency != null) {
            tracker.remove(outAdjacency.release());
            tracker.remove(outOffsets.release());
            outAdjacency = null;
            outWeights = null;
            outOffsets = null;
            outWeightOffsets = null;
        }
        emptyAdjacencyCursor = null;
        inCache = null;
        outCache = null;
    }

    @Override
    public void releaseProperties() {
        if (canRelease) {
            for (final WeightMapping nodeMapping : nodeProperties.values()) {
                tracker.remove(nodeMapping.release());
            }
        }
    }

    @Override
    public boolean isUndirected() {
        return isUndirected;
    }

    @Override
    public boolean hasRelationshipProperty() {
        return hasRelationshipProperty;
    }

    @Override
    public Direction getLoadDirection() {
        if (inOffsets != null && outOffsets != null) {
            return Direction.BOTH;
        } else if (inOffsets != null) {
            return Direction.INCOMING;
        } else {
            assert (outOffsets != null);
            return Direction.OUTGOING;
        }
    }

    private AdjacencyList.DecompressingCursor newAdjacencyCursor(final AdjacencyList adjacency) {
        return adjacency != null ? adjacency.rawDecompressingCursor() : null;
    }

    private int degree(long node, AdjacencyOffsets offsets, AdjacencyList array) {
        long offset = offsets.get(node);
        if (offset == 0L) {
            return 0;
        }
        return array.getDegree(offset);
    }

    private AdjacencyList.DecompressingCursor adjacencyCursor(
        long node,
        AdjacencyList.DecompressingCursor adjacencyCursor,
        AdjacencyOffsets offsets,
        AdjacencyList adjacencyList
    ) {
        if (offsets == null) {
            return emptyAdjacencyCursor;
        }
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return emptyAdjacencyCursor;
        }
        return adjacencyList.decompressingCursor(adjacencyCursor, offset);
    }

    private AdjacencyList.Cursor weightCursor(
        long node,
        AdjacencyOffsets offsets,
        AdjacencyList weights
    ) {
        if (!hasRelationshipProperty()) {
            throw new UnsupportedOperationException(
                "Can not create weight cursor on a graph without relationship property");
        }

        final long offset = offsets.get(node);
        if (offset == 0L) {
            return AdjacencyList.Cursor.EMPTY;
        }
        return weights.cursor(offset);
    }

    private void consumeAdjacentNodes(
        long startNode,
        AdjacencyList.DecompressingCursor adjacencyCursor,
        RelationshipConsumer consumer
    ) {
        while (adjacencyCursor.hasNextVLong()) {
            if (!consumer.accept(startNode, adjacencyCursor.nextVLong())) {
                break;
            }
        }
    }

    private void consumeAdjacentNodesWithWeight(
        long startNode,
        AdjacencyList.DecompressingCursor adjacencyCursor,
        AdjacencyList.Cursor weightCursor,
        WeightedRelationshipConsumer consumer
    ) {

        while (adjacencyCursor.hasNextVLong()) {
            long targetNodeId = adjacencyCursor.nextVLong();

            long weightBits = weightCursor.nextLong();
            double weight = Double.longBitsToDouble(weightBits);

            if (!consumer.accept(startNode, targetNodeId, weight)) {
                break;
            }
        }
    }

    static class GetTargetConsumer implements RelationshipConsumer {
        static final long TARGET_NOT_FOUND = -1L;

        private long count;
        private long target = TARGET_NOT_FOUND;

        GetTargetConsumer(long count) {
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

        ExistsConsumer(long targetNodeId) {
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
