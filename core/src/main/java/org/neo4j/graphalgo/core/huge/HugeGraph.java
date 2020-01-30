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
package org.neo4j.graphalgo.core.huge;

import org.jetbrains.annotations.Nullable;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.Relationships;
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

    public static final double NO_PROPERTY_VALUE = Double.NaN;
    public static final int NO_SUCH_NODE = 0;

    private final IdMap idMapping;
    private final AllocationTracker tracker;

    private final Map<String, NodeProperties> nodeProperties;
    private final long relationshipCount;
    private AdjacencyList inAdjacency;
    private AdjacencyList outAdjacency;
    private AdjacencyOffsets inOffsets;
    private AdjacencyOffsets outOffsets;

    private final double defaultPropertyValue;
    private AdjacencyList inProperties;
    private AdjacencyList outProperties;
    private AdjacencyOffsets inPropertyOffsets;
    private AdjacencyOffsets outPropertyOffsets;

    private AdjacencyList.DecompressingCursor emptyAdjacencyCursor;
    private AdjacencyList.DecompressingCursor inCache;
    private AdjacencyList.DecompressingCursor outCache;

    private boolean canRelease = true;

    private final boolean hasRelationshipProperty;
    private final boolean isUndirected;

    public static HugeGraph create(
        AllocationTracker tracker,
        IdMap idMapping,
        Map<String, NodeProperties> nodeProperties,
        @Nullable AdjacencyList outAdjacencyList,
        @Nullable AdjacencyOffsets outAdjacencyOffsets,
        @Nullable AdjacencyList inAdjacencyList,
        @Nullable AdjacencyOffsets inAdjacencyOffsets,
        long relationshipCount,
        boolean loadAsUndirected) {

        return create(
            tracker,
            idMapping,
            nodeProperties,
            relationshipCount,
            inAdjacencyList,
            outAdjacencyList,
            inAdjacencyOffsets,
            outAdjacencyOffsets,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            loadAsUndirected);
    }

    public static HugeGraph create(
        AllocationTracker tracker,
        IdMap idMapping,
        Map<String, NodeProperties> nodeProperties,
        long relationshipCount,
        @Nullable AdjacencyList inAdjacency,
        @Nullable AdjacencyList outAdjacency,
        @Nullable AdjacencyOffsets inOffsets,
        @Nullable AdjacencyOffsets outOffsets,
        Optional<Double> defaultPropertyValue,
        Optional<AdjacencyList> inProperties,
        Optional<AdjacencyList> outProperties,
        Optional<AdjacencyOffsets> inPropertyOffsets,
        Optional<AdjacencyOffsets> outPropertyOffsets,
        boolean isUndirected
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
            inProperties.isPresent() || outProperties.isPresent(),
            defaultPropertyValue.orElse(Double.NaN),
            inProperties.orElse(null),
            outProperties.orElse(null),
            inPropertyOffsets.orElse(null),
            outPropertyOffsets.orElse(null),
            isUndirected
        );
    }

    /**
     * Create a HugeGraph based on an existing graph but with different topology.
     * The new relationships must guarantee to connect nodes within the node space
     * of the base graph.
     */
    public static HugeGraph create(HugeGraph baseGraph, Relationships relationships, boolean isUndirected) {
        boolean hasRelationshipProperty = relationships.inRelProperties() != null || relationships.outRelProperties() != null;
        double defaultPropertyValue = relationships.maybeDefaultRelProperty().orElse(baseGraph.defaultPropertyValue);
        return new HugeGraph(
            baseGraph.tracker,
            baseGraph.idMapping,
            baseGraph.nodeProperties,
            relationships.relationshipCount(),
            relationships.inAdjacency(),
            relationships.outAdjacency(),
            relationships.inOffsets(),
            relationships.outOffsets(),
            hasRelationshipProperty,
            defaultPropertyValue,
            relationships.inRelProperties(),
            relationships.outRelProperties(),
            relationships.inRelPropertyOffsets(),
            relationships.outRelPropertyOffsets(),
            isUndirected
        );
    }

    public HugeGraph(
        AllocationTracker tracker,
        IdMap idMapping,
        Map<String, NodeProperties> nodeProperties,
        long relationshipCount,
        @Nullable AdjacencyList inAdjacency,
        @Nullable AdjacencyList outAdjacency,
        @Nullable AdjacencyOffsets inOffsets,
        @Nullable AdjacencyOffsets outOffsets,
        boolean hasRelationshipProperty,
        double defaultPropertyValue,
        @Nullable AdjacencyList inProperties,
        @Nullable AdjacencyList outProperties,
        @Nullable AdjacencyOffsets inPropertyOffsets,
        @Nullable AdjacencyOffsets outPropertyOffsets,
        boolean isUndirected
    ) {
        this.idMapping = idMapping;
        this.tracker = tracker;
        this.nodeProperties = nodeProperties;
        this.relationshipCount = relationshipCount;
        this.inAdjacency = inAdjacency;
        this.outAdjacency = outAdjacency;
        this.inOffsets = inOffsets;
        this.outOffsets = outOffsets;
        this.defaultPropertyValue = defaultPropertyValue;
        this.inProperties = inProperties;
        this.outProperties = outProperties;
        this.inPropertyOffsets = inPropertyOffsets;
        this.outPropertyOffsets = outPropertyOffsets;
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
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        return relationshipProperty(sourceNodeId, targetNodeId, defaultPropertyValue);
    }

    @Override
    public double relationshipProperty(final long sourceNodeId, final long targetNodeId, double fallbackValue) {
        if (!hasRelationshipProperty) {
            return fallbackValue;
        }

        double maybeValue;

        if (outProperties != null) {
            maybeValue = findPropertyValue(
                sourceNodeId,
                targetNodeId,
                outProperties,
                outPropertyOffsets,
                outAdjacency,
                outOffsets
            );
            if (!Double.isNaN(maybeValue)) {
                return maybeValue;
            }
        }

        if (inProperties != null) {
            maybeValue = findPropertyValue(targetNodeId, sourceNodeId, inProperties,
                inPropertyOffsets, inAdjacency, inOffsets
            );

            if (!Double.isNaN(maybeValue)) {
                return maybeValue;
            }
        }

        return defaultPropertyValue;
    }

    private double findPropertyValue(
        final long fromId,
        final long toId,
        final AdjacencyList properties,
        final AdjacencyOffsets propertyOffsets,
        final AdjacencyList adjacencies,
        final AdjacencyOffsets adjacencyOffsets
    ) {
        long relOffset = adjacencyOffsets.get(fromId);
        if (relOffset == NO_SUCH_NODE) {
            return NO_PROPERTY_VALUE;
        }
        long propertyOffset = propertyOffsets.get(fromId);

        AdjacencyList.DecompressingCursor relDecompressingCursor = adjacencies.decompressingCursor(relOffset);
        AdjacencyList.Cursor propertyCursor = properties.cursor(propertyOffset);

        while (relDecompressingCursor.hasNextVLong() && propertyCursor.hasNextLong() && relDecompressingCursor.nextVLong() != toId) {
            propertyCursor.nextLong();
        }

        if (!propertyCursor.hasNextLong()) {
            return NO_PROPERTY_VALUE;
        }

        long doubleBits = propertyCursor.nextLong();
        return Double.longBitsToDouble(doubleBits);
    }

    @Override
    public NodeProperties nodeProperties(final String type) {
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
                runForEach(nodeId, Direction.INCOMING, consumer);
                return;

            case OUTGOING:
                runForEach(nodeId, Direction.OUTGOING, consumer);
                return;

            default:
                runForEach(nodeId, Direction.OUTGOING, consumer);
                runForEach(nodeId, Direction.INCOMING, consumer);
        }
    }

    @Override
    public void forEachRelationship(
        long nodeId,
        Direction direction,
        double fallbackValue,
        RelationshipWithPropertyConsumer consumer
    ) {

        switch (direction) {
            case INCOMING:
                runForEachWithProperty(nodeId, Direction.INCOMING, fallbackValue, consumer);
                return;

            case OUTGOING:
                runForEachWithProperty(nodeId, Direction.OUTGOING, fallbackValue, consumer);
                return;

            default:
                runForEachWithProperty(nodeId, Direction.OUTGOING, fallbackValue, consumer);
                runForEachWithProperty(nodeId, Direction.INCOMING, fallbackValue, consumer);
        }
    }

    @Override
    public int degree(final long node) {
        return degree(node, outOffsets, outAdjacency);
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return idMapping.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return idMapping.toOriginalNodeId(nodeId);
    }

    public IdMap idMapping() {
        return idMapping;
    }

    @Override
    public boolean contains(final long nodeId) {
        return idMapping.contains(nodeId);
    }

    @Override
    public void forEachIncoming(long node, final RelationshipConsumer consumer) {
        runForEach(node, Direction.INCOMING, consumer);
    }

    @Override
    public void forEachOutgoing(long node, final RelationshipConsumer consumer) {
        runForEach(node, Direction.OUTGOING, consumer);
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
            defaultPropertyValue,
            inProperties,
            outProperties,
            inPropertyOffsets,
            outPropertyOffsets,
            isUndirected
        );
    }

    @Override
    public RelationshipIntersect intersection() {
        return new HugeGraphIntersectImpl(outAdjacency, outOffsets);
    }

    /**
     * O(n) !
     */
    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        ExistsConsumer consumer = new ExistsConsumer(targetNodeId);
        runForEach(sourceNodeId, Direction.OUTGOING, consumer);
        return consumer.found;
    }

    /*
     * O(n) !
     */
    @Override
    public long getTarget(long sourceNodeId, long index) {
        GetTargetConsumer consumer = new GetTargetConsumer(index);
        runForEach(sourceNodeId, Direction.OUTGOING, consumer);
        return consumer.target;
    }

    private void runForEach(
        long sourceNodeId,
        Direction direction,
        RelationshipConsumer consumer
    ) {

        if (direction == Direction.BOTH) {
            runForEach(sourceNodeId, Direction.OUTGOING, consumer);
            runForEach(sourceNodeId, Direction.INCOMING, consumer);
            return;
        }

        AdjacencyList.DecompressingCursor adjacencyCursor = adjacencyCursorForIteration(
            sourceNodeId,
            direction
        );
        consumeAdjacentNodes(sourceNodeId, adjacencyCursor, consumer);
    }

    private void runForEachWithProperty(
        long sourceNodeId,
        Direction direction,
        double fallbackValue,
        RelationshipWithPropertyConsumer consumer
    ) {

        if (direction == Direction.BOTH) {
            runForEachWithProperty(sourceNodeId, Direction.OUTGOING, fallbackValue, consumer);
            runForEachWithProperty(sourceNodeId, Direction.INCOMING, fallbackValue, consumer);
            return;
        }

        if (!hasRelationshipProperty()) {
            runForEach(sourceNodeId, direction, (s, t) -> consumer.accept(s, t, fallbackValue));
        } else {
            AdjacencyList.DecompressingCursor adjacencyCursor = adjacencyCursorForIteration(
                sourceNodeId,
                direction
            );

            AdjacencyList.Cursor propertyCursor = propertyCursorForIteration(sourceNodeId, direction);
            consumeAdjacentNodesWithProperty(sourceNodeId, adjacencyCursor, propertyCursor, consumer);
        }
    }

    private AdjacencyList.DecompressingCursor adjacencyCursorForIteration(
        long sourceNodeId,
        Direction direction
    ) {
        if (direction == Direction.OUTGOING) {
            return adjacencyCursor(
                sourceNodeId,
                outCache,
                outOffsets,
                outAdjacency
            );
        } else {
            return adjacencyCursor(
                sourceNodeId,
                inCache,
                inOffsets,
                inAdjacency
            );
        }
    }

    private AdjacencyList.Cursor propertyCursorForIteration(long sourceNodeId, Direction direction) {
        if (direction == Direction.OUTGOING) {
            return propertyCursor(
                sourceNodeId,
                outPropertyOffsets,
                outProperties
            );
        } else {
            return propertyCursor(
                sourceNodeId,
                inPropertyOffsets,
                inProperties
            );
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
            inProperties = null;
            inOffsets = null;
            inPropertyOffsets = null;
        }
        if (outAdjacency != null) {
            tracker.remove(outAdjacency.release());
            tracker.remove(outOffsets.release());
            outAdjacency = null;
            outProperties = null;
            outOffsets = null;
            outPropertyOffsets = null;
        }
        emptyAdjacencyCursor = null;
        inCache = null;
        outCache = null;
    }

    @Override
    public void releaseProperties() {
        if (canRelease) {
            for (final NodeProperties nodeMapping : nodeProperties.values()) {
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
    public HugeGraph withoutRelationshipProperties() {
        if (!hasRelationshipProperty()) {
            return this;
        } else {
            HugeGraph graphWithoutProperties = new HugeGraph(
                tracker,
                idMapping,
                nodeProperties,
                relationshipCount,
                inAdjacency,
                outAdjacency,
                inOffsets,
                outOffsets,
                false,
                Double.NaN,
                null,
                null,
                null,
                null,
                isUndirected
            );
            graphWithoutProperties.canRelease(canRelease);
            return graphWithoutProperties;
        }
    }

    private AdjacencyList.DecompressingCursor newAdjacencyCursor(final AdjacencyList adjacency) {
        return adjacency != null ? adjacency.rawDecompressingCursor() : null;
    }

    private int degree(long node, AdjacencyOffsets offsets, AdjacencyList array) {
        if (offsets == null) {
            return 0;
        }
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
            throw new NullPointerException();
        }
        final long offset = offsets.get(node);
        if (offset == 0L) {
            return emptyAdjacencyCursor;
        }
        return adjacencyList.decompressingCursor(adjacencyCursor, offset);
    }

    private AdjacencyList.Cursor propertyCursor(
        long node,
        AdjacencyOffsets offsets,
        AdjacencyList properties
    ) {
        if (!hasRelationshipProperty()) {
            throw new UnsupportedOperationException(
                "Can not create property cursor on a graph without relationship property");
        }

        final long offset = offsets.get(node);
        if (offset == 0L) {
            return AdjacencyList.Cursor.EMPTY;
        }
        return properties.cursor(offset);
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

    private void consumeAdjacentNodesWithProperty(
        long startNode,
        AdjacencyList.DecompressingCursor adjacencyCursor,
        AdjacencyList.Cursor propertyCursor,
        RelationshipWithPropertyConsumer consumer
    ) {

        while (adjacencyCursor.hasNextVLong()) {
            long targetNodeId = adjacencyCursor.nextVLong();

            long propertyBits = propertyCursor.nextLong();
            double property = Double.longBitsToDouble(propertyBits);

            if (!consumer.accept(startNode, targetNodeId, property)) {
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
