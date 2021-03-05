/*
 * Copyright (c) "Neo4j"
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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.AdjacencyCursor;
import org.neo4j.graphalgo.api.AdjacencyDegrees;
import org.neo4j.graphalgo.api.AdjacencyList;
import org.neo4j.graphalgo.api.AdjacencyOffsets;
import org.neo4j.graphalgo.api.CSRGraph;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.PropertyCursor;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.graphalgo.core.huge.TransientAdjacencyList.Cursor;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

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
 * (the neo kernel api returns an int for {@link org.neo4j.internal.kernel.api.helpers.Nodes#countAll}).
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
public class HugeGraph implements CSRGraph {

    public static final double NO_PROPERTY_VALUE = Double.NaN;
    private static final int NO_SUCH_NODE = 0;

    protected final NodeMapping idMapping;
    protected final AllocationTracker tracker;
    protected final GraphSchema schema;

    protected final Map<String, NodeProperties> nodeProperties;

    protected final Orientation orientation;

    protected final long relationshipCount;

    protected @NotNull AdjacencyList adjacencyList;
    protected @NotNull AdjacencyDegrees adjacencyDegrees;
    protected @NotNull AdjacencyOffsets adjacencyOffsets;

    protected final double defaultPropertyValue;
    @Nullable
    protected AdjacencyList properties;
    @Nullable
    protected AdjacencyOffsets propertyOffsets;

    private AdjacencyCursor emptyCursor;
    private AdjacencyCursor cursorCache;

    private boolean canRelease = true;

    protected final boolean hasRelationshipProperty;
    protected final boolean isMultiGraph;

    public static HugeGraph create(
        NodeMapping nodes,
        GraphSchema schema,
        Map<String, NodeProperties> nodeProperties,
        Relationships.Topology topology,
        Optional<Relationships.Properties> maybeProperties,
        AllocationTracker tracker
    ) {
        return new HugeGraph(
            nodes,
            schema,
            nodeProperties,
            topology.elementCount(),
            topology.list(),
            topology.degrees(),
            topology.offsets(),
            maybeProperties.isPresent(),
            maybeProperties.map(Relationships.Properties::defaultPropertyValue).orElse(Double.NaN),
            maybeProperties.map(Relationships.Properties::list).orElse(null),
            maybeProperties.map(Relationships.Properties::offsets).orElse(null),
            topology.orientation(),
            topology.isMultiGraph(),
            tracker
        );
    }

    public HugeGraph(
        NodeMapping idMapping,
        GraphSchema schema,
        Map<String, NodeProperties> nodeProperties,
        long relationshipCount,
        @NotNull AdjacencyList adjacencyList,
        @NotNull AdjacencyDegrees adjacencyDegrees,
        @NotNull AdjacencyOffsets adjacencyOffsets,
        boolean hasRelationshipProperty,
        double defaultPropertyValue,
        @Nullable AdjacencyList properties,
        @Nullable AdjacencyOffsets propertyOffsets,
        Orientation orientation,
        boolean isMultiGraph,
        AllocationTracker tracker
    ) {
        this.idMapping = idMapping;
        this.schema = schema;
        this.isMultiGraph = isMultiGraph;
        this.tracker = tracker;
        this.nodeProperties = nodeProperties;
        this.relationshipCount = relationshipCount;
        this.adjacencyList = adjacencyList;
        this.adjacencyDegrees = adjacencyDegrees;
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
    public long rootNodeCount() {
        return idMapping.rootNodeCount();
    }

    public NodeMapping idMap() {
        return idMapping;
    }

    @Override
    public GraphSchema schema() {
        return schema;
    }

    @Override
    public NodeMapping nodeMapping() {
        return idMapping;
    }

    public Map<String, NodeProperties> nodeProperties() { return nodeProperties; }

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

        int degree = adjacencyDegrees.degree(fromId);
        AdjacencyCursor relDecompressingCursor = adjacencyList.decompressingCursor(relOffset, degree);
        PropertyCursor propertyCursor = properties.cursor(propertyOffset, degree);

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
    public NodeProperties nodeProperties(String propertyKey) {
        return nodeProperties.get(propertyKey);
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
    public Stream<RelationshipCursor> streamRelationships(long nodeId, double fallbackValue) {
        var adjacencyCursor = adjacencyCursorForIteration(nodeId);
        var spliterator = !hasRelationshipProperty()
            ? AdjacencySpliterator.of(adjacencyCursor, nodeId, fallbackValue)
            : AdjacencySpliterator.of(adjacencyCursor, propertyCursorForIteration(nodeId), nodeId);

        return StreamSupport.stream(spliterator, false);
    }

    @Override
    public RelationshipIterator relationshipTypeFilteredIterator(Set<RelationshipType> relationshipTypes) {
        assertSupportedRelationships(relationshipTypes);
        return this;
    }

    @Override
    public Map<RelationshipType, Relationships.Topology> relationshipTopologies() {
        return Map.of(relationshipType(), relationshipTopology());
    }

    public Relationships.Topology relationshipTopology() {
        return relationships().topology();
    }

    private void assertSupportedRelationships(Set<RelationshipType> relationshipTypes) {
        if (!relationshipTypes.isEmpty() && (relationshipTypes.size() > 1 || !relationshipTypes.contains(relationshipType()))) {
            throw new IllegalArgumentException(formatWithLocale(
                "One or more relationship types of %s in are not supported. This graph has a relationship of type %s.",
                relationshipTypes,
                relationshipType()
            ));
        }
    }

    private RelationshipType relationshipType() {
        return availableRelationshipTypes().iterator().next();
    }

    @Override
    public int degree(long node) {
        return adjacencyDegrees.degree(node);
    }

    @Override
    public int degreeWithoutParallelRelationships(long nodeId) {
        if (!isMultiGraph()) {
            return degree(nodeId);
        }
        var degreeCounter = new ParallelRelationshipsDegreeCounter();
        runForEach(nodeId, degreeCounter);
        return degreeCounter.degree;
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
    public long toRootNodeId(long nodeId) {
        return idMapping.toRootNodeId(nodeId);
    }

    @Override
    public boolean contains(long nodeId) {
        return idMapping.contains(nodeId);
    }

    @Override
    public HugeGraph concurrentCopy() {
        return new HugeGraph(
            idMapping,
            schema,
            nodeProperties,
            relationshipCount,
            adjacencyList,
            adjacencyDegrees,
            adjacencyOffsets,
            hasRelationshipProperty,
            defaultPropertyValue,
            properties,
            propertyOffsets,
            orientation,
            isMultiGraph,
            tracker
        );
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
        var adjacencyCursor = adjacencyCursorForIteration(sourceId);
        consumeAdjacentNodes(sourceId, adjacencyCursor, consumer);
    }

    private void runForEach(long sourceId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        if (!hasRelationshipProperty()) {
            runForEach(sourceId, (s, t) -> consumer.accept(s, t, fallbackValue));
        } else {
            var adjacencyCursor = adjacencyCursorForIteration(sourceId);
            var propertyCursor = propertyCursorForIteration(sourceId);
            consumeAdjacentNodesWithProperty(sourceId, adjacencyCursor, propertyCursor, consumer);
        }
    }

    private AdjacencyCursor adjacencyCursorForIteration(long sourceNodeId) {
        long offset = adjacencyOffsets.get(sourceNodeId);
        if (offset == 0L) {
            return emptyCursor;
        }
        cursorCache.init(offset, adjacencyDegrees.degree(sourceNodeId));
        return cursorCache;
    }

    private PropertyCursor propertyCursorForIteration(long sourceNodeId) {
        if (!hasRelationshipProperty() || propertyOffsets == null || properties == null) {
            throw new UnsupportedOperationException(
                "Can not create property cursor on a graph without relationship property");
        }

        long offset = propertyOffsets.get(sourceNodeId);
        if (offset == 0L) {
            return Cursor.EMPTY;
        }
        return properties.cursor(offset, adjacencyDegrees.degree(sourceNodeId));
    }

    @Override
    public void canRelease(boolean canRelease) {
        this.canRelease = canRelease;
    }

    @Override
    public void releaseTopology() {
        if (!canRelease) return;

        adjacencyList.close();
        adjacencyList = null;
        adjacencyDegrees.close();
        adjacencyDegrees = null;
        adjacencyOffsets.close();
        adjacencyOffsets = null;
        if (properties != null) {
            properties.close();
            properties = null;
        }
        if (propertyOffsets != null) {
            propertyOffsets.close();
            propertyOffsets = null;
        }
        if (emptyCursor != null) {
            emptyCursor.close();
            emptyCursor = null;
        }
        if (cursorCache != null) {
            cursorCache.close();
            cursorCache = null;
        }
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

    @Override
    public boolean isMultiGraph() {
        return isMultiGraph;
    }

    public Relationships relationships() {
        return Relationships.of(
            relationshipCount,
            orientation,
            isMultiGraph(),
            adjacencyDegrees,
            adjacencyList,
            adjacencyOffsets,
            properties,
            propertyOffsets,
            defaultPropertyValue
        );
    }

    @Override
    public boolean hasRelationshipProperty() {
        return hasRelationshipProperty;
    }

    private AdjacencyCursor newAdjacencyCursor(AdjacencyList adjacency) {
        return adjacency != null ? adjacency.rawDecompressingCursor() : null;
    }

    private void consumeAdjacentNodes(
        long sourceId,
        AdjacencyCursor adjacencyCursor,
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
        AdjacencyCursor adjacencyCursor,
        PropertyCursor propertyCursor,
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

    public static class GetTargetConsumer implements RelationshipConsumer {
        static final long TARGET_NOT_FOUND = -1L;

        private long count;
        public long target = TARGET_NOT_FOUND;

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

    public static class ExistsConsumer implements RelationshipConsumer {
        private final long targetNodeId;
        public boolean found = false;

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

    private static class ParallelRelationshipsDegreeCounter implements RelationshipConsumer {
        private long previousNodeId;
        private int degree;

        ParallelRelationshipsDegreeCounter() {
            this.previousNodeId = -1;
        }

        @Override
        public boolean accept(long s, long t) {
            if (t != previousNodeId) {
                degree++;
                previousNodeId = t;
            }
            return true;
        }
    }
}
