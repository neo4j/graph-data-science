/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.pagecached;

import com.carrotsearch.hppc.BitSet;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.BiLongConsumer;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeSparseLongArray;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class IdMapBuilder {

    public static IdMap build(
        HugeLongArrayBuilder idMapBuilder,
        Map<NodeLabel, BitSet> labelInformation,
        long highestNodeId,
        int concurrency,
        AllocationTracker tracker
    ) throws IOException {
        long nodeCount = idMapBuilder.nodeCount();
        PagedFile graphIds = idMapBuilder.build();

        HugeSparseLongArray.Builder nodeMappingBuilder = HugeSparseLongArray.Builder.create(
            highestNodeId == 0 ? 1 : highestNodeId,
            tracker
        );
        var partitions = PartitionUtils.numberAlignedPartitioning(
            concurrency,
            nodeCount * Long.BYTES,
            PageCache.PAGE_SIZE
        );

        List<Runnable> tasks = partitions.stream().map(p -> (Runnable) () -> {
            try {
                var startPage = p.startNode / PageCache.PAGE_SIZE;
                var endPage = startPage + BitUtil.ceilDiv(p.nodeCount, PageCache.PAGE_SIZE);
                var pageCursor = graphIds.io(startPage, PagedFile.PF_SHARED_READ_LOCK, PageCursorTracer.NULL);
                var longsPerPage = PageCache.PAGE_SIZE / Long.BYTES;
                for (long pageId = startPage; pageId < endPage; pageId++) {
                    pageCursor.next(pageId);
                    for (int i = 0; i < longsPerPage; i++) {
                        var graphNodeId = pageId * longsPerPage + i;
                        var neoNodeId = pageCursor.getLong();
                        nodeMappingBuilder.set(neoNodeId, graphNodeId);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toList());

        ParallelUtil.run(tasks, Pools.DEFAULT);

        HugeSparseLongArray nodeToGraphIds = nodeMappingBuilder.build();
        return new IdMap(graphIds, nodeToGraphIds, labelInformation, nodeCount);
    }

//    static IdMap buildChecked(
//        HugeLongArrayBuilder idMapBuilder,
//        Map<NodeLabel, BitSet> labelInformation,
//        long highestNodeId,
//        int concurrency,
//        AllocationTracker tracker
//    ) throws DuplicateNodeIdException {
//        HugeLongArray graphIds = idMapBuilder.build();
//        HugeSparseLongArray nodeToGraphIds = buildSparseNodeMapping(
//            idMapBuilder.size(),
//            highestNodeId,
//            concurrency,
//            addChecked(graphIds),
//            tracker
//        );
//        return new IdMap(graphIds, nodeToGraphIds, labelInformation, idMapBuilder.size());
//    }

    @NotNull
    static HugeSparseLongArray buildSparseNodeMapping(
        long nodeCount,
        long highestNodeId,
        int concurrency,
        Function<HugeSparseLongArray.Builder, BiLongConsumer> nodeAdder,
        AllocationTracker tracker
    ) {
        HugeSparseLongArray.Builder nodeMappingBuilder = HugeSparseLongArray.Builder.create(
            highestNodeId == 0 ? 1 : highestNodeId,
            tracker
        );
        ParallelUtil.readParallel(concurrency, nodeCount, Pools.DEFAULT, nodeAdder.apply(nodeMappingBuilder));
        return nodeMappingBuilder.build();
    }

    static Function<HugeSparseLongArray.Builder, BiLongConsumer> add(HugeLongArray graphIds) {
        return builder -> (start, end) -> addNodes(graphIds, builder, start, end);
    }

//    private static Function<HugeSparseLongArray.Builder, BiLongConsumer> addChecked(HugeLongArray graphIds) {
//        return builder -> (start, end) -> addAndCheckNodes(graphIds, builder, start, end);
//    }

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

//    private static void addAndCheckNodes(
//        HugeLongArray graphIds,
//        HugeSparseLongArray.Builder builder,
//        long startNode,
//        long endNode
//    ) throws DuplicateNodeIdException {
//        try (HugeCursor<long[]> cursor = graphIds.initCursor(graphIds.newCursor(), startNode, endNode)) {
//            while (cursor.next()) {
//                long[] array = cursor.array;
//                int offset = cursor.offset;
//                int limit = cursor.limit;
//                long internalId = cursor.base + offset;
//                for (int i = offset; i < limit; ++i, ++internalId) {
//                    boolean addedAsNewId = builder.setIfAbsent(array[i], internalId);
//                    if (!addedAsNewId) {
//                        throw new DuplicateNodeIdException(array[i]);
//                    }
//                }
//            }
//        }
//    }

    private IdMapBuilder() {
    }
}
