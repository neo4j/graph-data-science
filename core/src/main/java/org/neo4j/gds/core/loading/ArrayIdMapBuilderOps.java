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
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.collections.HugeSparseLongArray;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.construction.NodesBuilder;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

public final class ArrayIdMapBuilderOps {

    static ArrayIdMap build(
        HugeLongArray internalToOriginalIds,
        long nodeCount,
        LabelInformation.Builder labelInformationBuilder,
        long highestNodeId,
        int concurrency
    ) {
        if (highestNodeId == NodesBuilder.UNKNOWN_MAX_ID) {
            highestNodeId = internalToOriginalIds.asNodeProperties().getMaxLongPropertyValue().orElse(NodesBuilder.UNKNOWN_MAX_ID);
        }

        HugeSparseLongArray originalToInternalIds = buildSparseIdMap(
            nodeCount,
            highestNodeId,
            concurrency,
            internalToOriginalIds
        );

        var labelInformation = labelInformationBuilder.build(nodeCount, originalToInternalIds::get);

        return new ArrayIdMap(
            internalToOriginalIds,
            originalToInternalIds,
            labelInformation,
            nodeCount,
            highestNodeId
        );
    }

    @NotNull
    static HugeSparseLongArray buildSparseIdMap(
        long nodeCount,
        long highestNodeId,
        int concurrency,
        HugeLongArray graphIds
    ) {
        HugeSparseLongArray.Builder idMapBuilder = HugeSparseLongArray.builder(
            IdMap.NOT_FOUND,
            // We need to allocate space for `highestNode + 1` since we
            // need to be able to store a node with `id = highestNodeId`.
            highestNodeId + 1
        );
        ParallelUtil.readParallel(
            concurrency,
            nodeCount,
            Pools.DEFAULT,
            (start, end) -> addNodes(graphIds, idMapBuilder, start, end)
        );
        return idMapBuilder.build();
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

    private ArrayIdMapBuilderOps() {
    }
}
