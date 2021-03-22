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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.api.BatchNodeIterable;
import org.neo4j.graphalgo.api.NodeIterator;
import org.neo4j.graphalgo.api.NodeMapping;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

/**
 * This is basically a long to int mapper. It sorts the id's in ascending order so its
 * guaranteed that there is no ID greater then nextGraphId / capacity
 */
public class IdMap implements NodeMapping, NodeIterator, BatchNodeIterable {

    private static final MemoryEstimation ESTIMATION = MemoryEstimations
        .builder(IdMap.class)
        .perNode("Neo4j identifiers", HugeLongArray::memoryEstimation)
        .rangePerGraphDimension(
            "Mapping from Neo4j identifiers to internal identifiers",
            (dimensions, concurrency) -> HugeSparseLongArray.memoryEstimation(
                dimensions.highestNeoId(),
                dimensions.nodeCount()
            )
        )
        .perGraphDimension(
            "Node Label BitSets",
            (dimensions, concurrency) ->
                MemoryRange.of(dimensions.estimationNodeLabelCount() * MemoryUsage.sizeOfBitset(dimensions.nodeCount()))
        )
        .build();

    private static final Set<NodeLabel> ALL_NODES_LABELS = Set.of(NodeLabel.ALL_NODES);

    private final long nodeCount;
    private final long highestNeoId;
    private final AllocationTracker tracker;

    private final Map<NodeLabel, BitSet> labelInformation;

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
        Map<NodeLabel, BitSet> labelInformation,
        long nodeCount,
        long highestNeoId,
        AllocationTracker tracker
    ) {
        this.graphIds = graphIds;
        this.nodeToGraphIds = nodeToGraphIds;
        this.labelInformation = labelInformation;
        this.nodeCount = nodeCount;
        this.highestNeoId = highestNeoId;
        this.tracker = tracker;
    }

    @Override
    public long toMappedNodeId(long nodeId) {
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
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        return LazyBatchCollection.of(
            nodeCount(),
            batchSize,
            IdIterable::new
        );
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return labelInformation.isEmpty()
            ? ALL_NODES_LABELS
            : labelInformation.keySet();
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        if (labelInformation.isEmpty()) {
            return ALL_NODES_LABELS;
        } else {
            Set<NodeLabel> set = new HashSet<>();
            for (var labelAndBitSet : labelInformation.entrySet()) {
                if (labelAndBitSet.getValue().get(nodeId)) {
                    set.add(labelAndBitSet.getKey());
                }
            }
            return set;
        }
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        if (labelInformation.isEmpty()) {
            consumer.accept(NodeLabel.ALL_NODES);
        } else {
            for (var labelAndBitSet : labelInformation.entrySet()) {
                if (labelAndBitSet.getValue().get(nodeId)) {
                    if (!consumer.accept(labelAndBitSet.getKey())) {
                        break;
                    }
                }
            }
        }
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        if (labelInformation.isEmpty() && label.equals(NodeLabel.ALL_NODES)) {
            return true;
        }
        BitSet bitSet = labelInformation.get(label);
        return bitSet != null && bitSet.get(nodeId);
    }

    @Override
    public IdMap withFilteredLabels(Collection<NodeLabel> nodeLabels, int concurrency) {
        validateNodeLabelFilter(nodeLabels, labelInformation);

        if (labelInformation.isEmpty()) {
            return this;
        }

        BitSet unionBitSet = new BitSet(nodeCount());
        nodeLabels.forEach(label -> unionBitSet.union(labelInformation.get(label)));

        long nodeId = -1L;
        long cursor = 0L;
        long newNodeCount = unionBitSet.cardinality();
        HugeLongArray newGraphIds = HugeLongArray.newArray(newNodeCount, tracker);

        while ((nodeId = unionBitSet.nextSetBit(nodeId + 1)) != -1) {
            newGraphIds.set(cursor, nodeId);
            cursor++;
        }

        HugeSparseLongArray newNodeToGraphIds = IdMapBuilder.buildSparseNodeMapping(
            newNodeCount,
            nodeToGraphIds.getCapacity(),
            concurrency,
            IdMapBuilder.add(newGraphIds),
            tracker
        );

        Map<NodeLabel, BitSet> newLabelInformation = nodeLabels
            .stream()
            .collect(Collectors.toMap(nodeLabel -> nodeLabel, labelInformation::get));

        return new FilteredIdMap(
            rootNodeCount(),
            newGraphIds,
            newNodeToGraphIds,
            newLabelInformation,
            newNodeCount,
            highestNeoId,
            tracker
        );
    }

    private void validateNodeLabelFilter(Collection<NodeLabel> nodeLabels, Map<NodeLabel, BitSet> labelInformation) {
        List<ElementIdentifier> invalidLabels = nodeLabels
            .stream()
            .filter(label -> !new HashSet<>(labelInformation.keySet()).contains(label))
            .collect(Collectors.toList());
        if (!invalidLabels.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale(
                "Specified labels %s do not correspond to any of the node projections %s.",
                invalidLabels,
                labelInformation.keySet()
            ));
        }
    }

    private static class FilteredIdMap extends IdMap {

        private final long rootNodeCount;

        FilteredIdMap(
            long rootNodeCount,
            HugeLongArray graphIds,
            HugeSparseLongArray nodeToGraphIds,
            Map<NodeLabel, BitSet> filteredLabelMap,
            long nodeCount,
            long highestNeoId,
            AllocationTracker tracker
        ) {
            super(graphIds, nodeToGraphIds, filteredLabelMap, nodeCount, highestNeoId, tracker);
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
