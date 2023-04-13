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
package org.neo4j.gds.core.compression.packed;

import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;

import java.lang.ref.Cleaner;

public class PackedAdjacencyList implements AdjacencyList {

    private static final Cleaner CLEANER = Cleaner.create();

    private final long[] pages;
    private final HugeIntArray degrees;
    private final HugeLongArray offsets;

    private final Cleaner.Cleanable cleanable;

    PackedAdjacencyList(long[] pages, int[] allocationSizes, HugeIntArray degrees, HugeLongArray offsets) {
        this.pages = pages;
        this.degrees = degrees;
        this.offsets = offsets;
        this.cleanable = CLEANER.register(this, new AdjacencyListCleaner(pages, allocationSizes));
    }

    @Override
    public int degree(long node) {
        return this.degrees.get(node);
    }

    @Override
    public AdjacencyCursor adjacencyCursor(long node, double fallbackValue) {
        var degree = this.degree(node);
        if (degree == 0) {
            return AdjacencyCursor.empty();
        }

        long offset = this.offsets.get(node);
        var cursor = new DecompressingCursor(this.pages);
        cursor.init(offset, degree);

        return cursor;
    }

    @Override
    public AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        var degree = this.degree(node);
        if (degree == 0) {
            return AdjacencyCursor.empty();
        }
        if (reuse instanceof DecompressingCursor) {
            long offset = this.offsets.get(node);
            reuse.init(offset, degree);
            return reuse;
        }
        return adjacencyCursor(node, fallbackValue);
    }

    @Override
    public AdjacencyCursor rawAdjacencyCursor() {
        return new DecompressingCursor(this.pages);
    }

    /**
     * Free the underlying memory.
     * <p>
     * This list cannot be used afterwards.
     * <p>
     * When this list is garbage collected, the memory is freed as well,
     * so it is not required to call this method to prevent memory leaks.
     */
    @TestOnly
    void free() {
        this.cleanable.clean();
    }

    private static class AdjacencyListCleaner implements Runnable {
        private final long[] pages;
        private final int[] allocationSizes;

        AdjacencyListCleaner(long[] pages, int[] allocationSizes) {
            this.pages = pages;
            this.allocationSizes = allocationSizes;
        }

        @Override
        public void run() {
            Address address = null;
            for (int pageIdx = 0; pageIdx < pages.length; pageIdx++) {
                if (address == null) {
                    address = Address.createAddress(pages[pageIdx], allocationSizes[pageIdx]);
                } else {
                    address.reset(pages[pageIdx], allocationSizes[pageIdx]);
                }
                address.free();
                pages[pageIdx] = 0;
            }
        }
    }
}

