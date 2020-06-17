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
package org.neo4j.gds.core.pagecached;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.compat.Neo4jProxy;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class IdMapBuilder {

    public static IdMap build(
        InternalToNeoIdMappingBuilder idMapBuilder,
        ReverseIdMappingBuilder.Builder nodeMappingBuilder,
        Map<NodeLabel, BitSet> labelInformation,
        int concurrency
    ) throws IOException {
        long nodeCount = idMapBuilder.nodeCount();
        PagedFile graphIds = idMapBuilder.build();

        var partitions = PartitionUtils.numberAlignedPartitioning(
            concurrency,
            nodeCount * Long.BYTES,
            PageCache.PAGE_SIZE
        );

        List<Runnable> tasks = partitions.stream().map(p -> (Runnable) () -> {
            try {
                var startPage = p.startNode / PageCache.PAGE_SIZE;
                var endPage = startPage + BitUtil.ceilDiv(p.nodeCount, PageCache.PAGE_SIZE);
                var pageCursor = Neo4jProxy.pageFileIO(
                    graphIds,
                    startPage,
                    PagedFile.PF_SHARED_READ_LOCK,
                    PageCursorTracer.NULL
                );
                try (pageCursor) {
                    var longsPerPage = PageCache.PAGE_SIZE / Long.BYTES;
                    for (long pageId = startPage; pageId < endPage; pageId++) {
                        pageCursor.next(pageId);
                        for (int i = 0; i < longsPerPage; i++) {
                            var graphNodeId = pageId * longsPerPage + i;
                            if (graphNodeId >= nodeCount) {
                                break;
                            }
                            var neoNodeId = pageCursor.getLong();
                            nodeMappingBuilder.set(neoNodeId, graphNodeId);
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toList());

        ParallelUtil.run(tasks, Pools.DEFAULT);

        ReverseIdMappingBuilder nodeToGraphIds = nodeMappingBuilder.build();
        return new IdMap(graphIds, nodeToGraphIds, labelInformation, nodeCount);
    }

    private IdMapBuilder() {
    }
}
