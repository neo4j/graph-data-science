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
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.Relationships;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
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
    private static final int NO_SUCH_NODE = 0;

    private final IdMap idMapping;
    private final AllocationTracker tracker;

    private final Map<String, NodeProperties> nodeProperties;

    private final Orientation orientation;

    private final long relationshipCount;
    private AdjacencyList adjacencyList;
    private AdjacencyOffsets adjacencyOffsets;

    private final double defaultPropertyValue;
    private @Nullable AdjacencyList properties;
    private @Nullable AdjacencyOffsets propertyOffsets;

    private AdjacencyList.DecompressingCursor emptyCursor;
    private AdjacencyList.DecompressingCursor cursorCache;

    private boolean canRelease = true;

    private final boolean hasRelationshipProperty;

    public static HugeGraph create(
        AllocationTracker tracker,
        IdMap nodes,
        Map<String, NodeProperties> nodeProperties,
        CSR relationships,
        Optional<PropertyCSR> maybeRelationshipProperties
    ) {
        return new HugeGraph(
            tracker,
            nodes,
            nodeProperties,
            relationships.elementCount(),
            relationships.list(),
            relationships.offsets(),
            maybeRelationshipProperties.isPresent(),
            maybeRelationshipProperties.map(PropertyCSR::defaultPropertyValue).orElse(Double.NaN),
            maybeRelationshipProperties.map(PropertyCSR::list).orElse(null),
            maybeRelationshipProperties.map(PropertyCSR::offsets).orElse(null),
            relationships.orientation()
        );
    }

    public HugeGraph(
        AllocationTracker tracker,
        IdMap idMapping,
        Map<String, NodeProperties> nodeProperties,
        long relationshipCount,
        AdjacencyList adjacencyList,
        AdjacencyOffsets adjacencyOffsets,
        boolean hasRelationshipProperty,
        double defaultPropertyValue,
        @Nullable AdjacencyList properties,
        @Nullable AdjacencyOffsets propertyOffsets,
        Orientation orientation
    ) {
        this.idMapping = idMapping;
        this.tracker = tracker;
        this.nodeProperties = nodeProperties;
        this.relationshipCount = relationshipCount;
        this.adjacencyList = adjacencyList;
        this.adjacencyOffsets = adjacencyOffsets;
        this.defaultPropertyValue = defaultPropertyValue;
        this.properties = properties;
        this.propertyOffsets = propertyOffsets;
        this.orientation = orientation;
        this.hasRelationshipProperty = hasRelationshipProperty;
        this.cursorCache = newAdjacencyCursor(this.adjacencyList);
        this.emptyCursor = newAdjacencyCursor(this.adjacencyList);
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
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
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
    public double relationshipProperty(long sourceId, long targetId, double fallbackValue) {
        if (!hasRelationshipProperty) {
            return fallbackValue;
        }

        double maybeValue;

        if (properties != null) {
            maybeValue = findPropertyValue(sourceId, targetId);
            if (!Double.isNaN(maybeValue)) {
                return maybeValue;
            }
        }

        return defaultPropertyValue;
    }

    private double findPropertyValue(long fromId, long toId) {
        long relOffset = adjacencyOffsets.get(fromId);
        if (relOffset == NO_SUCH_NODE) {
            return NO_PROPERTY_VALUE;
        }
        long propertyOffset = propertyOffsets.get(fromId);

        AdjacencyList.DecompressingCursor relDecompressingCursor = adjacencyList.decompressingCursor(relOffset);
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
    public NodeProperties nodeProperties(String type) {
        return nodeProperties.get(type);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return nodeProperties.keySet();
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        runForEach(nodeId, consumer);
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        runForEach(nodeId, fallbackValue, consumer);
    }

    @Override
    public int degree(long node) {
        if (adjacencyOffsets == null) {
            return 0;
        }
        long offset = adjacencyOffsets.get(node);
        if (offset == 0L) {
            return 0;
        }
        return adjacencyList.getDegree(offset);
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
    public boolean contains(long nodeId) {
        return idMapping.contains(nodeId);
    }

    @Override
    public HugeGraph concurrentCopy() {
        return new HugeGraph(
            tracker,
            idMapping,
            nodeProperties,
            relationshipCount,
            adjacencyList,
            adjacencyOffsets,
            hasRelationshipProperty,
            defaultPropertyValue,
            properties,
            propertyOffsets,
            orientation
        );
    }

    @Override
    public RelationshipIntersect intersection() {
        return new HugeGraphIntersectImpl(adjacencyList, adjacencyOffsets);
    }

    /**
     * O(n) !
     */
    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        ExistsConsumer consumer = new ExistsConsumer(targetNodeId);
        runForEach(sourceNodeId, consumer);
        return consumer.found;
    }

    /*
     * O(n) !
     */
    @Override
    public long getTarget(long sourceNodeId, long index) {
        GetTargetConsumer consumer = new GetTargetConsumer(index);
        runForEach(sourceNodeId, consumer);
        return consumer.target;
    }

    private void runForEach(long sourceId, RelationshipConsumer consumer) {
        AdjacencyList.DecompressingCursor adjacencyCursor = adjacencyCursorForIteration(sourceId);
        consumeAdjacentNodes(sourceId, adjacencyCursor, consumer);
    }

    private void runForEach(long sourceId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        if (!hasRelationshipProperty()) {
            runForEach(sourceId, (s, t) -> consumer.accept(s, t, fallbackValue));
        } else {
            AdjacencyList.DecompressingCursor adjacencyCursor = adjacencyCursorForIteration(sourceId);
            AdjacencyList.Cursor propertyCursor = propertyCursorForIteration(sourceId);
            consumeAdjacentNodesWithProperty(sourceId, adjacencyCursor, propertyCursor, consumer);
        }
    }

    private AdjacencyList.DecompressingCursor adjacencyCursorForIteration(long sourceNodeId) {
        if (adjacencyOffsets == null) {
            throw new NullPointerException();
        }
        long offset = adjacencyOffsets.get(sourceNodeId);
        if (offset == 0L) {
            return emptyCursor;
        }
        return adjacencyList.decompressingCursor(cursorCache, offset);

    }

    private AdjacencyList.Cursor propertyCursorForIteration(long sourceNodeId) {
        if (!hasRelationshipProperty()) {
            throw new UnsupportedOperationException(
                "Can not create property cursor on a graph without relationship property");
        }

        long offset = propertyOffsets.get(sourceNodeId);
        if (offset == 0L) {
            return AdjacencyList.Cursor.EMPTY;
        }
        return properties.cursor(offset);
    }

    @Override
    public void canRelease(boolean canRelease) {
        this.canRelease = canRelease;
    }

    @Override
    public void releaseTopology() {
        if (!canRelease) return;

        if (adjacencyList != null) {
            tracker.remove(adjacencyList.release());
            tracker.remove(adjacencyOffsets.release());
            adjacencyList = null;
            properties = null;
            adjacencyOffsets = null;
            propertyOffsets = null;
        }
        emptyCursor = null;
        cursorCache = null;
    }

    @Override
    public void releaseProperties() {
        if (canRelease) {
            for (NodeProperties nodeMapping : nodeProperties.values()) {
                tracker.remove(nodeMapping.release());
            }
        }
    }

    @Override
    public boolean isUndirected() {
        return orientation == Orientation.UNDIRECTED;
    }

    public Orientation orientation() {
        return orientation;
    }

    public Relationships relationships() {
        return new Relationships(relationshipCount, adjacencyList, adjacencyOffsets, properties, propertyOffsets);
    }

    @Override
    public boolean hasRelationshipProperty() {
        return hasRelationshipProperty;
    }

    public double defaultRelationshipProperty() {
        return defaultPropertyValue;
    }

    private AdjacencyList.DecompressingCursor newAdjacencyCursor(AdjacencyList adjacency) {
        return adjacency != null ? adjacency.rawDecompressingCursor() : null;
    }

    private void consumeAdjacentNodes(
        long sourceId,
        AdjacencyList.DecompressingCursor adjacencyCursor,
        RelationshipConsumer consumer
    ) {
        while (adjacencyCursor.hasNextVLong()) {
            if (!consumer.accept(sourceId, adjacencyCursor.nextVLong())) {
                break;
            }
        }
    }

    private void consumeAdjacentNodesWithProperty(
        long sourceId,
        AdjacencyList.DecompressingCursor adjacencyCursor,
        AdjacencyList.Cursor propertyCursor,
        RelationshipWithPropertyConsumer consumer
    ) {

        while (adjacencyCursor.hasNextVLong()) {
            long targetId = adjacencyCursor.nextVLong();

            long propertyBits = propertyCursor.nextLong();
            double property = Double.longBitsToDouble(propertyBits);

            if (!consumer.accept(sourceId, targetId, property)) {
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

    @ValueClass
    public interface CSR {
        AdjacencyList list();

        AdjacencyOffsets offsets();

        long elementCount();

        Orientation orientation();
    }

    @ValueClass
    public interface PropertyCSR extends CSR {
        double defaultPropertyValue();
    }
}
