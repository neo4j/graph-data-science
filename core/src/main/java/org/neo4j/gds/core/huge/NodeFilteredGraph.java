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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.LongPredicate;

public class NodeFilteredGraph extends CSRGraphAdapter {

    private static final int NO_DEGREE = -1;

    private final NodeMapping filteredIdMap;
    private long relationshipCount;
    private final HugeIntArray degreeCache;

    public NodeFilteredGraph(CSRGraph originalGraph, NodeMapping filteredIdMap, AllocationTracker allocationTracker) {
        super(originalGraph);
        this.relationshipCount = -1;
        this.filteredIdMap = filteredIdMap;
        this.degreeCache = HugeIntArray.newArray(filteredIdMap.nodeCount(), allocationTracker);

        degreeCache.fill(NO_DEGREE);
    }

    public NodeFilteredGraph(CSRGraph originalGraph, NodeMapping filteredIdMap, HugeIntArray degreeCache) {
        super(originalGraph);

        this.degreeCache = degreeCache;
        this.filteredIdMap = filteredIdMap;
    }

    public NodeMapping nodeMapping() {
        return filteredIdMap;
    }

    @Override
    public GraphSchema schema() {
        return graph.schema().filterNodeLabels(filteredIdMap.availableNodeLabels());
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return filteredIdMap.nodeIterator();
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

        forEachRelationship(nodeId, (s, t) -> {
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
    public long rootNodeCount() {
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
        ParallelUtil.runWithConcurrency(ConcurrencyConfig.DEFAULT_CONCURRENCY, tasks, Pools.DEFAULT);

        this.relationshipCount = tasks.stream().mapToLong(RelationshipCounter::relationshipCount).sum();
    }

    @Override
    public long highestNeoId() {
        return filteredIdMap.highestNeoId();
    }

    @Override
    public long unsafeToMappedNodeId(long neoNodeId) {
        return filteredIdMap.unsafeToMappedNodeId(super.unsafeToMappedNodeId(neoNodeId));
    }

    @Override
    public long toRootNodeId(long nodeId) {
        return filteredIdMap.toRootNodeId(nodeId);
    }

    @Override
    public boolean contains(long nodeId) {
        return filteredIdMap.contains(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return super.toOriginalNodeId(filteredIdMap.toOriginalNodeId(nodeId));
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        super.forEachRelationship(filteredIdMap.toOriginalNodeId(nodeId), (s, t) -> filterAndConsume(s, t, consumer));
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        super.forEachRelationship(filteredIdMap.toOriginalNodeId(nodeId), fallbackValue, (s, t, p) -> filterAndConsume(s, t, p, consumer));
    }

    @Override
    public long getTarget(long sourceNodeId, long index) {
        HugeGraph.GetTargetConsumer consumer = new HugeGraph.GetTargetConsumer(index);
        forEachRelationship(sourceNodeId, consumer);
        return consumer.target;
    }

    public long getFilteredMappedNodeId(long nodeId) {
        return filteredIdMap.unsafeToMappedNodeId(nodeId);
    }

    public long getIntermediateOriginalNodeId(long nodeId) {
        return filteredIdMap.toOriginalNodeId(nodeId);
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        return super.exists(filteredIdMap.toOriginalNodeId(sourceNodeId), filteredIdMap.toOriginalNodeId(targetNodeId));
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        return super.relationshipProperty(filteredIdMap.toOriginalNodeId(sourceNodeId), filteredIdMap.toOriginalNodeId(targetNodeId), fallbackValue);
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        return super.relationshipProperty(filteredIdMap.toOriginalNodeId(sourceNodeId), filteredIdMap.toOriginalNodeId(targetNodeId));
    }

    @Override
    public CSRGraph concurrentCopy() {
        return new NodeFilteredGraph(graph.concurrentCopy(), filteredIdMap, degreeCache);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return filteredIdMap.availableNodeLabels();
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        return filteredIdMap.nodeLabels(nodeId);
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        return filteredIdMap.hasLabel(nodeId, label);
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        filteredIdMap.forEachNodeLabel(nodeId, consumer);
    }

    @Override
    public NodeProperties nodeProperties(String propertyKey) {
        NodeProperties properties = graph.nodeProperties(propertyKey);
        if (properties == null) {
            return null;
        }
        return new FilteredNodeProperties.FilteredToOriginalNodeProperties(properties, this);
    }

    private boolean filterAndConsume(long source, long target, RelationshipConsumer consumer) {
        if (filteredIdMap.contains(source) && filteredIdMap.contains(target)) {
            long internalSourceId = filteredIdMap.unsafeToMappedNodeId(source);
            long internalTargetId = filteredIdMap.unsafeToMappedNodeId(target);
            return consumer.accept(internalSourceId, internalTargetId);
        }
        return true;
    }

    private boolean filterAndConsume(long source, long target, double propertyValue, RelationshipWithPropertyConsumer consumer) {
        if (filteredIdMap.contains(source) && filteredIdMap.contains(target)) {
            long internalSourceId = filteredIdMap.unsafeToMappedNodeId(source);
            long internalTargetId = filteredIdMap.unsafeToMappedNodeId(target);
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
