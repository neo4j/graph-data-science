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

import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.BiLongConsumer;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class IdMapBuilder {

    public static BitIdMap build(
        InternalBitIdMappingBuilder idMapBuilder,
        Map<NodeLabel, HugeAtomicBitSet> labelInformation,
        AllocationTracker tracker
    ) {
        SparseLongArray graphIds = idMapBuilder.build();
        var convertedLabelInformation = labelInformation.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().toBitSet()
        ));

        return new BitIdMap(
            graphIds,
            convertedLabelInformation,
            tracker
        );
    }

    public static BitIdMap build(
        InternalSequentialBitIdMappingBuilder idMapBuilder,
        Map<NodeLabel, HugeAtomicBitSet> labelInformation,
        AllocationTracker tracker
    ) {
        SparseLongArray graphIds = idMapBuilder.build();
        var convertedLabelInformation = labelInformation.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().toBitSet()
        ));

        return new BitIdMap(
            graphIds,
            convertedLabelInformation,
            tracker
        );
    }

    public static IdMap build(
        InternalHugeIdMappingBuilder idMapBuilder,
        Map<NodeLabel, HugeAtomicBitSet> labelInformation,
        long highestNodeId,
        int concurrency,
        AllocationTracker tracker
    ) {
        HugeLongArray graphIds = idMapBuilder.build();
        HugeSparseLongArray nodeToGraphIds = buildSparseNodeMapping(
            idMapBuilder.size(),
            highestNodeId,
            concurrency,
            add(graphIds),
            tracker
        );

        var convertedLabelInformation = labelInformation.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().toBitSet()
        ));

        return new IdMap(
            graphIds,
            nodeToGraphIds,
            convertedLabelInformation,
            idMapBuilder.size(),
            highestNodeId,
            tracker
        );
    }

    static IdMap buildChecked(
        InternalHugeIdMappingBuilder idMapBuilder,
        Map<NodeLabel, HugeAtomicBitSet> labelInformation,
        long highestNodeId,
        int concurrency,
        AllocationTracker tracker
    ) throws DuplicateNodeIdException {
        HugeLongArray graphIds = idMapBuilder.build();
        HugeSparseLongArray nodeToGraphIds = buildSparseNodeMapping(
            idMapBuilder.size(),
            highestNodeId,
            concurrency,
            addChecked(graphIds),
            tracker
        );

        var convertedLabelInformation = labelInformation.entrySet().stream().collect(Collectors.toMap(
            Map.Entry::getKey,
            e -> e.getValue().toBitSet()
        ));

        return new IdMap(graphIds, nodeToGraphIds, convertedLabelInformation, idMapBuilder.size(), idMapBuilder.capacity(), tracker);
    }

    @NotNull
    static HugeSparseLongArray buildSparseNodeMapping(
        long nodeCount,
        long highestNodeId,
        int concurrency,
        Function<HugeSparseLongArray.Builder, BiLongConsumer> nodeAdder,
        AllocationTracker tracker
    ) {
        HugeSparseLongArray.Builder nodeMappingBuilder = HugeSparseLongArray.builder(
            // We need to allocate space for `highestNode + 1` since we
            // need to be able to store a node with `id = highestNodeId`.
            highestNodeId + 1,
            tracker
        );
        ParallelUtil.readParallel(concurrency, nodeCount, Pools.DEFAULT, nodeAdder.apply(nodeMappingBuilder));
        return nodeMappingBuilder.build();
    }

    public static Function<HugeSparseLongArray.Builder, BiLongConsumer> add(HugeLongArray graphIds) {
        return builder -> (start, end) -> addNodes(graphIds, builder, start, end);
    }

    private static Function<HugeSparseLongArray.Builder, BiLongConsumer> addChecked(HugeLongArray graphIds) {
        return builder -> (start, end) -> addAndCheckNodes(graphIds, builder, start, end);
    }

    private static void addNodes(
        HugeLongArray graphIds,
        HugeSparseLongArray.Builder builder,
        long startNode,
        long endNode
    ) {
        try (HugeCursor<long[]> cursor = graphIds.initCursor(graphIds.newCursor(), startNode, endNode)) {
            while (cursor.next()) {
                long[] array = cursor.array;
                int offset = cursor.offset;
                int limit = cursor.limit;
                long internalId = cursor.base + offset;
                for (int i = offset; i < limit; ++i, ++internalId) {
                    builder.set(array[i], internalId);
                }
            }
        }
    }

    private static void addAndCheckNodes(
        HugeLongArray graphIds,
        HugeSparseLongArray.Builder builder,
        long startNode,
        long endNode
    ) throws DuplicateNodeIdException {
        try (HugeCursor<long[]> cursor = graphIds.initCursor(graphIds.newCursor(), startNode, endNode)) {
            while (cursor.next()) {
                long[] array = cursor.array;
                int offset = cursor.offset;
                int limit = cursor.limit;
                long internalId = cursor.base + offset;
                for (int i = offset; i < limit; ++i, ++internalId) {
                    boolean addedAsNewId = builder.setIfAbsent(array[i], internalId);
                    if (!addedAsNewId) {
                        throw new DuplicateNodeIdException(array[i]);
                    }
                }
            }
        }
    }

    private IdMapBuilder() {
    }
}
