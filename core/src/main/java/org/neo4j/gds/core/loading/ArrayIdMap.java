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
import org.neo4j.gds.api.FilteredIdMap;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.LabeledIdMap;
import org.neo4j.gds.collections.HugeSparseCollections;
import org.neo4j.gds.collections.HugeSparseLongArray;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * This is basically a long to int mapper. It sorts the id's in ascending order so its
 * guaranteed that there is no ID greater then nextGraphId / capacity
 */
public class ArrayIdMap extends LabeledIdMap {

    private static final MemoryEstimation ESTIMATION = MemoryEstimations
        .builder(ArrayIdMap.class)
        .perNode("Neo4j identifiers", HugeLongArray::memoryEstimation)
        .rangePerGraphDimension(
            "Mapping from Neo4j identifiers to internal identifiers",
            (dimensions, concurrency) -> HugeSparseCollections.estimateLong(
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

    private final long highestNeoId;

    private final HugeLongArray graphIds;
    private final HugeSparseLongArray nodeToGraphIds;

    public static MemoryEstimation memoryEstimation() {
        return ESTIMATION;
    }

    /**
     * initialize the map with pre-built sub arrays
     */
    public ArrayIdMap(
        HugeLongArray graphIds,
        HugeSparseLongArray nodeToGraphIds,
        LabelInformation labelInformation,
        long nodeCount,
        long highestNeoId
    ) {
        super(labelInformation, nodeCount);
        this.graphIds = graphIds;
        this.nodeToGraphIds = nodeToGraphIds;
        this.highestNeoId = highestNeoId;
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return nodeToGraphIds.get(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return graphIds.get(mappedNodeId);
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return mappedNodeId;
    }

    @Override
    public IdMap rootIdMap() {
        return this;
    }

    @Override
    public boolean contains(final long originalNodeId) {
        return nodeToGraphIds.contains(originalNodeId);
    }

    @Override
    public OptionalLong rootNodeCount() {
        return OptionalLong.of(nodeCount());
    }

    @Override
    public long highestNeoId() {
        return highestNeoId;
    }

    @Override
    public Optional<FilteredArrayIdMap> withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        labelInformation.validateNodeLabelFilter(nodeLabels);

        if (labelInformation.isEmpty()) {
            return Optional.empty();
        }

        BitSet unionBitSet = labelInformation.unionBitSet(nodeLabels, nodeCount());

        long nodeId = -1L;
        long cursor = 0L;
        long newNodeCount = unionBitSet.cardinality();
        HugeLongArray newGraphIds = HugeLongArray.newArray(newNodeCount);

        while ((nodeId = unionBitSet.nextSetBit(nodeId + 1)) != -1) {
            newGraphIds.set(cursor, nodeId);
            cursor++;
        }

        HugeSparseLongArray newNodeToGraphIds = ArrayIdMapBuilderOps.buildSparseIdMap(
            newNodeCount,
            nodeToGraphIds.capacity(),
            concurrency,
            newGraphIds
        );

        LabelInformation newLabelInformation = labelInformation.filter(nodeLabels);

        return Optional.of(new FilteredArrayIdMap(
            this,
            newGraphIds,
            newNodeToGraphIds,
            newLabelInformation,
            newNodeCount,
            highestNeoId
        ));
    }

    private static class FilteredArrayIdMap extends ArrayIdMap implements FilteredIdMap {

        private final IdMap rootIdMap;

        FilteredArrayIdMap(
            IdMap rootIdMap,
            HugeLongArray graphIds,
            HugeSparseLongArray nodeToGraphIds,
            LabelInformation filteredLabelInformation,
            long nodeCount,
            long highestNeoId
        ) {
            super(graphIds, nodeToGraphIds, filteredLabelInformation, nodeCount, highestNeoId);
            this.rootIdMap = rootIdMap;
        }

        @Override
        public List<NodeLabel> nodeLabels(long mappedNodeId) {
            return super.nodeLabels(super.toOriginalNodeId(mappedNodeId));
        }

        @Override
        public void forEachNodeLabel(long mappedNodeId, NodeLabelConsumer consumer) {
            super.forEachNodeLabel(super.toOriginalNodeId(mappedNodeId), consumer);
        }

        @Override
        public OptionalLong rootNodeCount() {
            return rootIdMap.rootNodeCount();
        }

        @Override
        public long toRootNodeId(long mappedNodeId) {
            return super.toOriginalNodeId(mappedNodeId);
        }

        @Override
        public long rootToMappedNodeId(long rootNodeId) {
            return super.toMappedNodeId(rootNodeId);
        }

        @Override
        public long toOriginalNodeId(long mappedNodeId) {
            return rootIdMap.toOriginalNodeId(super.toOriginalNodeId(mappedNodeId));
        }

        @Override
        public long toMappedNodeId(long originalNodeId) {
            return super.toMappedNodeId(rootIdMap.toMappedNodeId(originalNodeId));
        }

        @Override
        public boolean contains(long originalNodeId) {
            return super.contains(rootIdMap.toMappedNodeId(originalNodeId));
        }

        @Override
        public boolean containsRootNodeId(long rootNodeId) {
            return super.contains(rootNodeId);
        }

        @Override
        public IdMap rootIdMap() {
            return rootIdMap.rootIdMap();
        }

        @Override
        public boolean hasLabel(long mappedNodeId, NodeLabel label) {
            return super.hasLabel(super.toOriginalNodeId(mappedNodeId), label);
        }
    }
}
