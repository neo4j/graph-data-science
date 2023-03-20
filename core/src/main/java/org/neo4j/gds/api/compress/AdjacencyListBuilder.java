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
package org.neo4j.gds.api.compress;

import org.immutables.value.Value;
import org.neo4j.gds.core.utils.PageReordering;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.utils.GdsFeatureToggles;

public interface AdjacencyListBuilder<PAGE, T> {

    Allocator<PAGE> newAllocator();

    PositionalAllocator<PAGE> newPositionalAllocator();

    T build(HugeIntArray degrees, HugeLongArray offsets);

    // TODO: extra interface for the positional allocator
    interface Allocator<PAGE> extends AutoCloseable {

        long allocate(int length, Slice<PAGE> into);

        @Override
        void close();
    }

    interface PositionalAllocator<PAGE> extends AutoCloseable {

        void writeAt(long address, PAGE targets, int length);

        @Override
        void close();
    }

    @Value.Modifiable
    interface Slice<PAGE> {

        PAGE slice();

        int offset();

        int length();
    }

    default void reorder(PAGE[] pages, HugeLongArray offsets, HugeIntArray degrees) {
        if (GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.isEnabled() && pages.length > 0) {
            PageReordering.reorder(pages, offsets, degrees);
        }
    }

}
