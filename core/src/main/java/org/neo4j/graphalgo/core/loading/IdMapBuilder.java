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
package org.neo4j.graphalgo.core.loading;

import com.carrotsearch.hppc.BitSet;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.ElementIdentifier;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;

import java.util.Map;
import java.util.Optional;

public final class IdMapBuilder {

    public static IdMap build(
        HugeLongArrayBuilder idMapBuilder,
        Map<ElementIdentifier, BitSet> elementIdentifierLabelMapping,
        long highestNodeId,
        int concurrency,
        AllocationTracker tracker
    ) {
        Optional<Map<ElementIdentifier, BitSet>> maybeLabelInformation = elementIdentifierLabelMapping == null || elementIdentifierLabelMapping.isEmpty()
            ? Optional.empty()
            : Optional.of(elementIdentifierLabelMapping);
        HugeLongArray graphIds = idMapBuilder.build();

        SparseNodeMapping nodeToGraphIds = buildSparseNodeMapping(graphIds, highestNodeId, concurrency, tracker);
        return new IdMap(graphIds, nodeToGraphIds, maybeLabelInformation, idMapBuilder.size());
    }

    @NotNull
    static SparseNodeMapping buildSparseNodeMapping(
        HugeLongArray graphIds,
        long highestNodeId,
        int concurrency,
        AllocationTracker tracker
    ) {
        SparseNodeMapping.Builder nodeMappingBuilder = SparseNodeMapping.Builder.create(highestNodeId == 0 ? 1 : highestNodeId, tracker);
        ParallelUtil.readParallel(
                concurrency,
                graphIds.size(),
                Pools.DEFAULT,
                (start, end) -> {
                    try (HugeCursor<long[]> cursor = graphIds.initCursor(graphIds.newCursor(), start, end)) {
                        while (cursor.next()) {
                            long[] array = cursor.array;
                            int offset = cursor.offset;
                            int limit = cursor.limit;
                            long internalId = cursor.base + offset;
                            for (int i = offset; i < limit; ++i, ++internalId) {
                                nodeMappingBuilder.set(array[i], internalId);
                            }
                        }
                    }
                }
        );

        return nodeMappingBuilder.build();
    }

    private IdMapBuilder() {
    }
}
