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
package org.neo4j.gds.core.loading;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.collections.HugeSparseArrays;
import org.neo4j.gds.collections.HugeSparseLongArray;
import org.neo4j.gds.core.utils.LazyBatchCollection;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;

/**
 * This is basically a long to int mapper. It sorts the id's in ascending order so its
 * guaranteed that there is no ID greater then nextGraphId / capacity
 */
public class IdMap implements NodeMapping {

    private static final MemoryEstimation ESTIMATION = MemoryEstimations
        .builder(IdMap.class)
        .perNode("Neo4j identifiers", HugeLongArray::memoryEstimation)
        .rangePerGraphDimension(
            "Mapping from Neo4j identifiers to internal identifiers",
            (dimensions, concurrency) -> HugeSparseArrays.estimateLong(
                dimensions.highestPossibleNodeCount(),
                dimensions.nodeCount()
            )
        )
        .perGraphDimension(
            "Node Label BitSets",
            (dimensions, concurrency) ->
                MemoryRange.of(dimensions.estimationNodeLabelCount() * MemoryUsage.sizeOfBitset(dimensions.nodeCount()))
        )
        .build();

    private final long nodeCount;
    private final long highestNeoId;
    private final AllocationTracker allocationTracker;

    private final LabelInformation labelInformation;

    private final HugeLongArray graphIds;
    private final HugeSparseLongArray nodeToGraphIds;

    public static MemoryEstimation memoryEstimation() {
        return ESTIMATION;
    }

    /**
     * initialize the map with pre-built sub arrays
     */
    public IdMap(
        HugeLongArray graphIds,
        HugeSparseLongArray nodeToGraphIds,
        LabelInformation labelInformation,
        long nodeCount,
        long highestNeoId,
        AllocationTracker allocationTracker
    ) {
        this.graphIds = graphIds;
        this.nodeToGraphIds = nodeToGraphIds;
        this.labelInformation = labelInformation;
        this.nodeCount = nodeCount;
        this.highestNeoId = highestNeoId;
        this.allocationTracker = allocationTracker;
    }

    @Override
    public long unsafeToMappedNodeId(long nodeId) {
        return nodeToGraphIds.get(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return graphIds.get(nodeId);
    }

    @Override
    public long toRootNodeId(long nodeId) {
        return nodeId;
    }

    @Override
    public boolean contains(final long nodeId) {
        return nodeToGraphIds.contains(nodeId);
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public long rootNodeCount() {
        return nodeCount;
    }

    @Override
    public long highestNeoId() {
        return highestNeoId;
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        final long count = nodeCount();
        for (long i = 0L; i < count; i++) {
            if (!consumer.test(i)) {
                return;
            }
        }
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return new IdIterator(nodeCount());
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(long batchSize) {
        return LazyBatchCollection.of(
            nodeCount(),
            batchSize,
            IdIterable::new
        );
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return labelInformation.availableNodeLabels();
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        return labelInformation.nodeLabelsForNodeId(nodeId);
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        labelInformation.forEachNodeLabel(nodeId, consumer);
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        return labelInformation.hasLabel(nodeId, label);
    }

    @Override
    public IdMap withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        labelInformation.validateNodeLabelFilter(nodeLabels);

        if (labelInformation.isEmpty()) {
            return this;
        }

        BitSet unionBitSet = labelInformation.unionBitSet(nodeLabels, nodeCount());

        long nodeId = -1L;
        long cursor = 0L;
        long newNodeCount = unionBitSet.cardinality();
        HugeLongArray newGraphIds = HugeLongArray.newArray(newNodeCount, allocationTracker);

        while ((nodeId = unionBitSet.nextSetBit(nodeId + 1)) != -1) {
            newGraphIds.set(cursor, nodeId);
            cursor++;
        }

        HugeSparseLongArray newNodeToGraphIds = IdMapBuilder.buildSparseNodeMapping(
            newNodeCount,
            nodeToGraphIds.capacity(),
            concurrency,
            IdMapBuilder.add(newGraphIds),
            allocationTracker
        );

        LabelInformation newLabelInformation = labelInformation.filter(nodeLabels);

        return new FilteredIdMap(
            rootNodeCount(),
            newGraphIds,
            newNodeToGraphIds,
            newLabelInformation,
            newNodeCount,
            highestNeoId,
            allocationTracker
        );
    }

    private static class FilteredIdMap extends IdMap {

        private final long rootNodeCount;

        FilteredIdMap(
            long rootNodeCount,
            HugeLongArray graphIds,
            HugeSparseLongArray nodeToGraphIds,
            LabelInformation filteredLabelInformation,
            long nodeCount,
            long highestNeoId,
            AllocationTracker allocationTracker
        ) {
            super(graphIds, nodeToGraphIds, filteredLabelInformation, nodeCount, highestNeoId, allocationTracker);
            this.rootNodeCount = rootNodeCount;
        }

        @Override
        public Set<NodeLabel> nodeLabels(long nodeId) {
            return super.nodeLabels(toOriginalNodeId(nodeId));
        }

        @Override
        public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
            super.forEachNodeLabel(toOriginalNodeId(nodeId), consumer);
        }

        @Override
        public long rootNodeCount() {
            return rootNodeCount;
        }

        @Override
        public long toRootNodeId(long nodeId) {
            return super.toRootNodeId(toOriginalNodeId(nodeId));
        }

        @Override
        public boolean hasLabel(long nodeId, NodeLabel label) {
            return super.hasLabel(toOriginalNodeId(nodeId), label);
        }
    }
}
