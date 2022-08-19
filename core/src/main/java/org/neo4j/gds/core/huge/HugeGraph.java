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
package org.neo4j.gds.core.huge;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.PropertyCursor;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

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

    static final double NO_PROPERTY_VALUE = Double.NaN;

    protected final IdMap idMap;

    protected final GraphSchema schema;

    protected final Map<String, NodePropertyValues> nodeProperties;

    protected final Orientation orientation;

    protected final long relationshipCount;

    protected AdjacencyList adjacency;

    private final double defaultPropertyValue;
    protected @Nullable AdjacencyProperties properties;

    private AdjacencyCursor adjacencyCursorCache;

    private PropertyCursor propertyCursorCache;

    private boolean canRelease = true;

    protected final boolean hasRelationshipProperty;
    protected final boolean isMultiGraph;

    public static HugeGraph create(
        IdMap nodes,
        GraphSchema schema,
        Map<String, NodePropertyValues> nodeProperties,
        Relationships.Topology topology,
        Optional<Relationships.Properties> maybeRelationshipProperty
    ) {
        return new HugeGraph(
            nodes,
            schema,
            nodeProperties,
            topology.elementCount(),
            topology.adjacencyList(),
            maybeRelationshipProperty.isPresent(),
            maybeRelationshipProperty.map(Relationships.Properties::defaultPropertyValue).orElse(Double.NaN),
            maybeRelationshipProperty.map(Relationships.Properties::propertiesList).orElse(null),
            topology.orientation(),
            topology.isMultiGraph()
        );
    }

    protected HugeGraph(
        IdMap idMap,
        GraphSchema schema,
        Map<String, NodePropertyValues> nodeProperties,
        long relationshipCount,
        @NotNull AdjacencyList adjacency,
        boolean hasRelationshipProperty,
        double defaultRelationshipPropertyValue,
        @Nullable AdjacencyProperties relationshipProperty,
        Orientation orientation,
        boolean isMultiGraph
    ) {
        this.idMap = idMap;
        this.schema = schema;
        this.isMultiGraph = isMultiGraph;
        this.nodeProperties = nodeProperties;
        this.relationshipCount = relationshipCount;
        this.adjacency = adjacency;
        this.defaultPropertyValue = defaultRelationshipPropertyValue;
        this.properties = relationshipProperty;
        this.orientation = orientation;
        this.hasRelationshipProperty = hasRelationshipProperty;
        this.adjacencyCursorCache = adjacency.rawAdjacencyCursor();
        this.propertyCursorCache = relationshipProperty != null ? relationshipProperty.rawPropertyCursor() : null;
    }

    @Override
    public long nodeCount() {
        return idMap.nodeCount();
    }

    @Override
    public long nodeCount(NodeLabel nodeLabel) {
        return idMap.nodeCount(nodeLabel);
    }

    @Override
    public OptionalLong rootNodeCount() {
        return idMap.rootNodeCount();
    }

    @Override
    public long highestNeoId() {
        return idMap.highestNeoId();
    }

    public IdMap idMap() {
        return idMap;
    }

    @Override
    public IdMap rootIdMap() {
        return idMap.rootIdMap();
    }

    @Override
    public GraphSchema schema() {
        return schema;
    }

    public Map<String, NodePropertyValues> nodeProperties() { return nodeProperties; }

    @Override
    public long relationshipCount() {
        return relationshipCount;
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(long batchSize) {
        return idMap.batchIterables(batchSize);
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        idMap.forEachNode(consumer);
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator() {
        return idMap.nodeIterator();
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator(Set<NodeLabel> labels) {
        return idMap.nodeIterator(labels);
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
        var properties = Objects.requireNonNull(this.properties);

        var adjacencyCursor = adjacency.adjacencyCursor(fromId);
        if (!adjacencyCursor.hasNextVLong()) {
            return NO_PROPERTY_VALUE;
        }

        var propertyCursor = properties.propertyCursor(fromId, defaultPropertyValue);

        while (adjacencyCursor.hasNextVLong() && propertyCursor.hasNextLong() && adjacencyCursor.nextVLong() != toId) {
            propertyCursor.nextLong();
        }

        if (!propertyCursor.hasNextLong()) {
            return NO_PROPERTY_VALUE;
        }

        long doubleBits = propertyCursor.nextLong();
        return Double.longBitsToDouble(doubleBits);
    }

    @Override
    public NodePropertyValues nodeProperties(String propertyKey) {
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
    public Graph relationshipTypeFilteredGraph(Set<RelationshipType> relationshipTypes) {
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
        return schema().relationshipSchema().availableTypes().iterator().next();
    }

    @Override
    public int degree(long node) {
        return adjacency.degree(node);
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
    public long toMappedNodeId(long originalNodeId) {
        return idMap.toMappedNodeId(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return idMap.toOriginalNodeId(mappedNodeId);
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return idMap.toRootNodeId(mappedNodeId);
    }

    @Override
    public boolean contains(long originalNodeId) {
        return idMap.contains(originalNodeId);
    }

    @Override
    public HugeGraph concurrentCopy() {
        return new HugeGraph(
            idMap,
            schema,
            nodeProperties,
            relationshipCount,
            adjacency,
            hasRelationshipProperty,
            defaultPropertyValue,
            properties,
            orientation,
            isMultiGraph
        );
    }

    @Override
    public Optional<NodeFilteredGraph> asNodeFilteredGraph() {
        return Optional.empty();
    }

    /**
     * O(n) !
     */
    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        var cursor = adjacencyCursorForIteration(sourceNodeId);
        return cursor.advance(targetNodeId) == targetNodeId;
    }

    @Override
    public long nthTarget(long nodeId, int offset) {
        if (offset >= degree(nodeId)) {
            return NOT_FOUND;
        }

        var cursor = adjacencyCursorForIteration(nodeId);
        return cursor.advanceBy(offset);
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
        return adjacency.adjacencyCursor(adjacencyCursorCache, sourceNodeId);
    }

    private PropertyCursor propertyCursorForIteration(long sourceNodeId) {
        if (!hasRelationshipProperty() || properties == null) {
            throw new UnsupportedOperationException(
                "Can not create property cursor on a graph without relationship property");
        }

        return properties.propertyCursor(propertyCursorCache, sourceNodeId, defaultPropertyValue);
    }

    @Override
    public void canRelease(boolean canRelease) {
        this.canRelease = canRelease;
    }

    @Override
    public void releaseTopology() {
        if (!canRelease) return;

        if (adjacency != null) {
            adjacency.close();
            adjacency = null;
        }
        if (properties != null) {
            properties.close();
            properties = null;
        }
        if (adjacencyCursorCache != null) {
            adjacencyCursorCache.close();
            adjacencyCursorCache = null;
        }
        if (propertyCursorCache != null) {
            propertyCursorCache.close();
            propertyCursorCache = null;
        }
    }

    @Override
    public void releaseProperties() {
        if (canRelease) {
            for (NodePropertyValues idMap : nodeProperties.values()) {
                idMap.release();
            }
        }
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
            adjacency,
            properties,
            defaultPropertyValue
        );
    }

    @Override
    public boolean hasRelationshipProperty() {
        return hasRelationshipProperty;
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

    @Override
    public List<NodeLabel> nodeLabels(long mappedNodeId) {
        return idMap.nodeLabels(mappedNodeId);
    }

    @Override
    public void forEachNodeLabel(long mappedNodeId, NodeLabelConsumer consumer) {
        idMap.forEachNodeLabel(mappedNodeId, consumer);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return idMap.availableNodeLabels();
    }

    @Override
    public boolean hasLabel(long mappedNodeId, NodeLabel label) {
        return idMap.hasLabel(mappedNodeId, label);
    }

    @Override
    public Optional<? extends FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        return idMap.withFilteredLabels(nodeLabels, concurrency);
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
