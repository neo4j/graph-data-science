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

import org.neo4j.graphalgo.core.utils.PageReordering;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeIntArray;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.utils.GdsFeatureToggles;

import static org.neo4j.graphalgo.core.loading.BumpAllocator.PAGE_SHIFT;

public interface CsrListBuilder<PAGE, T> {

    Allocator<PAGE> newAllocator();

    T build(HugeIntArray degrees, HugeLongArray offsets);

    void flush();

    interface Allocator<PAGE> extends AutoCloseable {

        long write(PAGE targets, int length);

        @Override
        void close();
    }

    default HugeLongArray reorder(PAGE[] pages, HugeLongArray offsets, HugeIntArray degrees, AllocationTracker tracker) {
        if (GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.isEnabled() && pages.length > 0) {
            var ordering = PageReordering.ordering(offsets, nodeId -> degrees.get(nodeId) > 0, pages.length, PAGE_SHIFT);
            PageReordering.reorder(pages, ordering.ordering());
            return PageReordering.sortOffsets(offsets, ordering, node -> degrees.get(node) > 0, tracker);
        }
        return offsets;
    }
}
