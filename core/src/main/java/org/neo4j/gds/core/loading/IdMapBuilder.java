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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.BiLongConsumer;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeSparseLongArray;

import java.util.function.Function;

public final class IdMapBuilder {

    public static BitIdMap build(
        InternalBitIdMappingBuilder idMapBuilder,
        LabelInformation.Builder labelInformationBuilder,
        AllocationTracker allocationTracker
    ) {
        var idMap = idMapBuilder.build();
        var labelInformation = labelInformationBuilder.build(idMap.idCount(), idMap::toMappedNodeId);

        return new BitIdMap(
            idMap,
            labelInformation,
            allocationTracker
        );
    }

    public static BitIdMap build(
        InternalSequentialBitIdMappingBuilder idMapBuilder,
        LabelInformation.Builder labelInformationBuilder,
        AllocationTracker allocationTracker
    ) {
        var idMap = idMapBuilder.build();
        var labelInformation = labelInformationBuilder.build(idMap.idCount(), idMap::toMappedNodeId);

        return new BitIdMap(
            idMap,
            labelInformation,
            allocationTracker
        );
    }

    public static IdMap build(
        InternalHugeIdMappingBuilder idMapBuilder,
        LabelInformation.Builder labelInformationBuilder,
        long highestNodeId,
        int concurrency,
        AllocationTracker allocationTracker
    ) {
        HugeLongArray graphIds = idMapBuilder.build();
        HugeSparseLongArray nodeToGraphIds = buildSparseNodeMapping(
            idMapBuilder.size(),
            highestNodeId,
            concurrency,
            add(graphIds),
            allocationTracker
        );

        var nodeCount = idMapBuilder.size();
        var labelInformation = labelInformationBuilder.build(nodeCount, nodeToGraphIds::get);

        return new IdMap(
            graphIds,
            nodeToGraphIds,
            labelInformation,
            nodeCount,
            highestNodeId,
            allocationTracker
        );
    }

    static IdMap buildChecked(
        InternalHugeIdMappingBuilder idMapBuilder,
        LabelInformation.Builder labelInformationBuilder,
        long highestNodeId,
        int concurrency,
        AllocationTracker allocationTracker
    ) throws DuplicateNodeIdException {
        HugeLongArray graphIds = idMapBuilder.build();
        HugeSparseLongArray nodeToGraphIds = buildSparseNodeMapping(
            idMapBuilder.size(),
            highestNodeId,
            concurrency,
            addChecked(graphIds),
            allocationTracker
        );

        var nodeCount = idMapBuilder.size();
        var labelInformation = labelInformationBuilder.build(nodeCount, nodeToGraphIds::get);

        return new IdMap(
            graphIds,
            nodeToGraphIds,
            labelInformation,
            nodeCount,
            idMapBuilder.capacity(),
            allocationTracker
        );
    }

    @NotNull
    static HugeSparseLongArray buildSparseNodeMapping(
        long nodeCount,
        long highestNodeId,
        int concurrency,
        Function<HugeSparseLongArray.Builder, BiLongConsumer> nodeAdder,
        AllocationTracker allocationTracker
    ) {
        HugeSparseLongArray.Builder nodeMappingBuilder = HugeSparseLongArray.builder(
            // We need to allocate space for `highestNode + 1` since we
            // need to be able to store a node with `id = highestNodeId`.
            highestNodeId + 1,
            allocationTracker
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
