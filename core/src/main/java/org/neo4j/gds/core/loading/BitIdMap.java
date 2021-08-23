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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.api.BatchNodeIterable;
import org.neo4j.gds.api.NodeIterator;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.core.utils.LazyBatchCollection;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.mem.MemoryUsage;
import org.neo4j.gds.core.utils.paged.SparseLongArray;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;

/**
 * This is basically a long to int mapper. It sorts the id's in ascending order so its
 * guaranteed that there is no ID greater then nextGraphId / capacity
 */
public class BitIdMap implements NodeMapping, NodeIterator, BatchNodeIterable {

    private static final MemoryEstimation ESTIMATION = MemoryEstimations
        .builder(BitIdMap.class)
        .add("Mapping between Neo4j identifiers and internal identifiers", SparseLongArray.memoryEstimation())
        .perGraphDimension(
            "Node Label BitSets",
            (dimensions, concurrency) ->
                MemoryRange.of(dimensions.estimationNodeLabelCount() * MemoryUsage.sizeOfBitset(dimensions.nodeCount()))
        )
        .build();

    private final AllocationTracker tracker;

    private final LabelInformation labelInformation;

    public static MemoryEstimation memoryEstimation() {
        return ESTIMATION;
    }

    private final SparseLongArray sparseLongArray;

    /**
     * initialize the map with pre-built sub arrays
     */
    public BitIdMap(
        SparseLongArray sparseLongArray,
        LabelInformation labelInformation,
        AllocationTracker tracker
    ) {
        this.sparseLongArray = sparseLongArray;
        this.labelInformation = labelInformation;
        this.tracker = tracker;
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return sparseLongArray.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return sparseLongArray.toOriginalNodeId(nodeId);
    }

    @Override
    public long toRootNodeId(long nodeId) {
        return nodeId;
    }

    @Override
    public boolean contains(final long nodeId) {
        return sparseLongArray.contains(nodeId);
    }

    @Override
    public long nodeCount() {
        return sparseLongArray.idCount();
    }

    @Override
    public long rootNodeCount() {
        return sparseLongArray.idCount();
    }

    @Override
    public long highestNeoId() {
        return sparseLongArray.highestNeoId();
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
    public BitIdMap withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        labelInformation.validateNodeLabelFilter(nodeLabels);

        if (labelInformation.isEmpty()) {
            return this;
        }

        var unionBitSet = labelInformation.unionBitSet(nodeLabels, nodeCount());

        var sparseLongArray = SparseLongArray.fromExistingBuilder(unionBitSet.bits).build();

        LabelInformation newLabelInformation = labelInformation.filter(nodeLabels);

        return new FilteredIdMap(
            rootNodeCount(),
            sparseLongArray,
            newLabelInformation,
            tracker
        );
    }

    private static class FilteredIdMap extends BitIdMap {

        private final long rootNodeCount;

        FilteredIdMap(
            long rootNodeCount,
            SparseLongArray sparseLongArray,
            LabelInformation filteredLabelInformation,
            AllocationTracker tracker
        ) {
            super(sparseLongArray, filteredLabelInformation, tracker);
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
