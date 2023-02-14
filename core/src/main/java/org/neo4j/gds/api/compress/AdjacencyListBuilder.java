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

import org.neo4j.gds.core.utils.PageReordering;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.utils.GdsFeatureToggles;

public interface AdjacencyListBuilder<PAGE, T> {

    Allocator<PAGE> newAllocator();

    Allocator<PAGE> newPositionalAllocator();

    T build(HugeIntArray degrees, HugeLongArray offsets);

    interface Allocator<PAGE> extends AutoCloseable {

        long write(PAGE targets, int length, long address);

        @Override
        void close();
    }

    default void reorder(PAGE[] pages, HugeLongArray offsets, HugeIntArray degrees) {
        if (GdsFeatureToggles.USE_REORDERED_ADJACENCY_LIST.isEnabled() && pages.length > 0) {
            PageReordering.reorder(pages, offsets, degrees);
        }
    }

}
