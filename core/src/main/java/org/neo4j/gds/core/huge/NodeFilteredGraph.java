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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.CSRGraphAdapter;
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.ImmutableRelationshipCursor;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.utils.CloseableThreadLocal;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Stream;

public class NodeFilteredGraph extends CSRGraphAdapter implements FilteredIdMap {

    private static final int NO_DEGREE = -1;

    private final FilteredIdMap filteredIdMap;
    private long relationshipCount;
    private final HugeIntArray degreeCache;
    private final CloseableThreadLocal<Graph> threadLocalGraph;

    public NodeFilteredGraph(CSRGraph originalGraph, FilteredIdMap filteredIdMap) {
        this(originalGraph, filteredIdMap, emptyDegreeCache(filteredIdMap), -1);
    }

    private NodeFilteredGraph(CSRGraph originalGraph, FilteredIdMap filteredIdMap, HugeIntArray degreeCache, long relationshipCount) {
        super(originalGraph);

        this.degreeCache = degreeCache;
        this.filteredIdMap = filteredIdMap;
        this.relationshipCount = relationshipCount;
        this.threadLocalGraph = CloseableThreadLocal.withInitial(this::concurrentCopy);
    }

    private static HugeIntArray emptyDegreeCache(IdMap filteredIdMap) {
        var degreeCache = HugeIntArray.newArray(filteredIdMap.nodeCount());
        degreeCache.fill(NO_DEGREE);
        return degreeCache;
    }

    @Override
    public GraphSchema schema() {
        return csrGraph.schema().filterNodeLabels(filteredIdMap.availableNodeLabels());
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator() {
        return filteredIdMap.nodeIterator();
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator(Set<NodeLabel> labels) {
        return filteredIdMap.nodeIterator(labels);
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(long batchSize) {
        return filteredIdMap.batchIterables(batchSize);
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        filteredIdMap.forEachNode(consumer);
    }

    @Override
    public int degree(long nodeId) {
        int cachedDegree = degreeCache.get(nodeId);
        if (cachedDegree != NO_DEGREE) {
            return cachedDegree;
        }

        var degree = new MutableInt();

        threadLocalGraph.get().forEachRelationship(nodeId, (s, t) -> {
            degree.increment();
            return true;
        });
        degreeCache.set(nodeId, degree.intValue());

        return degree.intValue();
    }

    @Override
    public int degreeWithoutParallelRelationships(long nodeId) {
        var degreeCounter = new NonDuplicateRelationshipsDegreeCounter();

        // iterates only over valid relationships
        forEachRelationship(nodeId, degreeCounter);

        return degreeCounter.degree;
    }

    @Override
    public long nodeCount() {
        return filteredIdMap.nodeCount();
    }

    @Override
    public long nodeCount(NodeLabel nodeLabel) {
        return filteredIdMap.nodeCount(nodeLabel);
    }

    @Override
    public OptionalLong rootNodeCount() {
        return filteredIdMap.rootNodeCount();
    }

    @Override
    public long relationshipCount() {
        if (relationshipCount == -1) {
            doCount();
        }
        return relationshipCount;
    }

    private void doCount() {
        var tasks = PartitionUtils.rangePartition(
            ConcurrencyConfig.DEFAULT_CONCURRENCY,
            nodeCount(),
            partition -> new RelationshipCounter(concurrentCopy(), partition),
            Optional.empty()
        );
        RunWithConcurrency.builder()
            .concurrency(ConcurrencyConfig.DEFAULT_CONCURRENCY)
            .tasks(tasks)
            .run();

        this.relationshipCount = tasks.stream().mapToLong(RelationshipCounter::relationshipCount).sum();
    }

    @Override
    public long highestOriginalId() {
        return filteredIdMap.highestOriginalId();
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return filteredIdMap.toMappedNodeId(originalNodeId);
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return filteredIdMap.toRootNodeId(mappedNodeId);
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return filteredIdMap.toOriginalNodeId(mappedNodeId);
    }


    @Override
    public long toFilteredNodeId(long rootNodeId) {
        return filteredIdMap.toFilteredNodeId(rootNodeId);
    }

    @Override
    public boolean contains(long originalNodeId) {
        return filteredIdMap.contains(originalNodeId);
    }

    @Override
    public boolean containsRootNodeId(long rootNodeId) {
        return filteredIdMap.containsRootNodeId(rootNodeId);
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        super.forEachRelationship(filteredIdMap.toRootNodeId(nodeId), (s, t) -> filterAndConsume(s, t, consumer));
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        super.forEachRelationship(
            filteredIdMap.toRootNodeId(nodeId),
            fallbackValue,
            (s, t, p) -> filterAndConsume(s, t, p, consumer)
        );
    }

    @Override
    public Stream<RelationshipCursor> streamRelationships(long nodeId, double fallbackValue) {
        return super.streamRelationships(filteredIdMap.toRootNodeId(nodeId), fallbackValue)
            .filter(rel -> filteredIdMap.containsRootNodeId(rel.sourceId()) && filteredIdMap.containsRootNodeId(rel.targetId()))
            .map(rel -> ImmutableRelationshipCursor.of(
                filteredIdMap.toFilteredNodeId(rel.sourceId()),
                filteredIdMap.toFilteredNodeId(rel.targetId()),
                rel.property()
            ));
    }

    @Override
    public Optional<NodeFilteredGraph> asNodeFilteredGraph() {
        return Optional.of(this);
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        return super.exists(filteredIdMap.toRootNodeId(sourceNodeId), filteredIdMap.toRootNodeId(targetNodeId));
    }

    @Override
    public long nthTarget(long nodeId, int offset) {
        return Graph.nthTarget(this, nodeId, offset);
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        return super.relationshipProperty(
            filteredIdMap.toRootNodeId(sourceNodeId),
            filteredIdMap.toRootNodeId(targetNodeId),
            fallbackValue
        );
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        return super.relationshipProperty(
            filteredIdMap.toRootNodeId(sourceNodeId),
            filteredIdMap.toRootNodeId(targetNodeId)
        );
    }

    @Override
    public CSRGraph concurrentCopy() {
        return new NodeFilteredGraph(csrGraph.concurrentCopy(), filteredIdMap, degreeCache, relationshipCount);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return filteredIdMap.availableNodeLabels();
    }

    @Override
    public List<NodeLabel> nodeLabels(long mappedNodeId) {
        return filteredIdMap.nodeLabels(mappedNodeId);
    }

    @Override
    public boolean hasLabel(long mappedNodeId, NodeLabel label) {
        return filteredIdMap.hasLabel(mappedNodeId, label);
    }

    @Override
    public void forEachNodeLabel(long mappedNodeId, NodeLabelConsumer consumer) {
        filteredIdMap.forEachNodeLabel(mappedNodeId, consumer);
    }

    @Override
    public Optional<FilteredIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        return filteredIdMap.withFilteredLabels(nodeLabels, concurrency);
    }

    @Override
    public NodePropertyValues nodeProperties(String propertyKey) {
        NodePropertyValues properties = csrGraph.nodeProperties(propertyKey);
        if (properties == null) {
            return null;
        }
        return new FilteredNodePropertyValues.FilteredToOriginalNodePropertyValues(properties, this);
    }

    @Override
    public void release() {
        super.release();
        this.threadLocalGraph.close();
    }

    private boolean filterAndConsume(long source, long target, RelationshipConsumer consumer) {
        if (filteredIdMap.containsRootNodeId(source) && filteredIdMap.containsRootNodeId(target)) {
            long internalSourceId = filteredIdMap.toFilteredNodeId(source);
            long internalTargetId = filteredIdMap.toFilteredNodeId(target);
            return consumer.accept(internalSourceId, internalTargetId);
        }
        return true;
    }

    private boolean filterAndConsume(long source, long target, double propertyValue, RelationshipWithPropertyConsumer consumer) {
        if (filteredIdMap.containsRootNodeId(source) && filteredIdMap.containsRootNodeId(target)) {
            long internalSourceId = filteredIdMap.toFilteredNodeId(source);
            long internalTargetId = filteredIdMap.toFilteredNodeId(target);
            return consumer.accept(internalSourceId, internalTargetId, propertyValue);
        }
        return true;
    }

    static class RelationshipCounter implements Runnable {
        private long relationshipCount;
        private final Graph graph;
        private final Partition partition;

        RelationshipCounter(Graph graph, Partition partition) {
            this.partition = partition;
            this.graph = graph;
            this.relationshipCount = 0;
        }

        @Override
        public void run() {
            partition.consume(nodeId -> graph.forEachRelationship(nodeId, (src, target) -> {
                // we call our filter-safe iterator and count every time
                relationshipCount++;
                return true;
            }));
        }

        long relationshipCount() {
            return relationshipCount;
        }
    }

    private static class NonDuplicateRelationshipsDegreeCounter implements RelationshipConsumer {
        private long previousNodeId;
        private int degree;

        NonDuplicateRelationshipsDegreeCounter() {
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
