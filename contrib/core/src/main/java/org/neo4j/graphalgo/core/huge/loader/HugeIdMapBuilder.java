/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayBuilder;
import org.neo4j.graphalgo.core.utils.paged.SparseLongArray;

final class HugeIdMapBuilder {

    static HugeIdMap build(
            HugeLongArrayBuilder idMapBuilder,
            long highestNodeId,
            AllocationTracker tracker) {
        HugeLongArray graphIds = idMapBuilder.build();
        SparseLongArray nodeToGraphIds = SparseLongArray.newArray(highestNodeId, tracker);

        try (HugeCursor<long[]> cursor = graphIds.cursor(graphIds.newCursor())) {
            while (cursor.next()) {
                long[] array = cursor.array;
                int offset = cursor.offset;
                int limit = cursor.limit;
                long internalId = cursor.base + offset;
                for (int i = offset; i < limit; ++i, ++internalId) {
                    nodeToGraphIds.set(array[i], internalId);
                }
            }
        }

        return new HugeIdMap(graphIds, nodeToGraphIds, idMapBuilder.size());
    }

    private HugeIdMapBuilder() {
    }
}
