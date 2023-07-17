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
import org.neo4j.gds.utils.GdsFeatureToggles;

import java.lang.ref.Cleaner;

public class PackedAdjacencyList implements AdjacencyList {

    private static final Cleaner CLEANER = Cleaner.create();

    private final long[] pages;
    private final HugeIntArray degrees;
    private final HugeLongArray offsets;

    private final MemoryInfo memoryInfo;
    private final Cleaner.Cleanable cleanable;


    // temp
    private interface NewCursor {
        AdjacencyCursor newCursor(long offset, int degree, long[] pages);
    }

    private interface NewReuseCursor {
        AdjacencyCursor newCursor(@Nullable AdjacencyCursor reuse, long offset, int degree, long[] pages);
    }

    private interface NewRawCursor {
        AdjacencyCursor newRawCursor(long[] pages);
    }

    /**
     * Block-aligned-tail cursor methods.
     */

    private static AdjacencyCursor newCursorWithBlockAlignedTail(long offset, int degree, long[] pages) {
        var cursor = new BlockAlignedTailCursor(pages);
        cursor.init(offset, degree);
        return cursor;
    }

    private static AdjacencyCursor newReuseCursorWithBlockAlignedTail(
        @Nullable AdjacencyCursor reuse,
        long offset,
        int degree,
        long[] pages
    ) {
        if (reuse instanceof BlockAlignedTailCursor) {
            reuse.init(offset, degree);
            return reuse;
        } else {
            var cursor = new BlockAlignedTailCursor(pages);
            cursor.init(offset, degree);
            return cursor;
        }
    }

    private static AdjacencyCursor newRawCursorWithBlockAlignedTail(long[] pages) {
        return new BlockAlignedTailCursor(pages);
    }

    /**
     * Packed-tail cursor methods.
     */

    private static AdjacencyCursor newCursorWithPackedTail(long offset, int degree, long[] pages) {
        var cursor = new PackedTailCursor(pages);
        cursor.init(offset, degree);
        return cursor;
    }

    private static AdjacencyCursor newReuseCursorWithPackedTail(
        @Nullable AdjacencyCursor reuse,
        long offset,
        int degree,
        long[] pages
    ) {
        if (reuse instanceof PackedTailCursor) {
            reuse.init(offset, degree);
            return reuse;
        } else {
            var cursor = new PackedTailCursor(pages);
            cursor.init(offset, degree);
            return cursor;
        }
    }

    private static AdjacencyCursor newRawCursorWithPackedTail(long[] pages) {
        return new PackedTailCursor(pages);
    }

    /**
     * Var-long-tail cursor methods.
     */

    private static AdjacencyCursor newCursorWithVarLongTail(long offset, int degree, long[] pages) {
        var cursor = new VarLongTailCursor(pages);
        cursor.init(offset, degree);
        return cursor;
    }

    private static AdjacencyCursor newReuseCursorWithVarLengthTail(
        @Nullable AdjacencyCursor reuse,
        long offset,
        int degree,
        long[] pages
    ) {
        if (reuse instanceof VarLongTailCursor) {
            reuse.init(offset, degree);
            return reuse;
        } else {
            var cursor = new VarLongTailCursor(pages);
            cursor.init(offset, degree);
            return cursor;
        }
    }

    private static AdjacencyCursor newRawCursorWithVarLongTail(long[] pages) {
        return new VarLongTailCursor(pages);
    }

    private final NewCursor newCursor;
    private final NewReuseCursor newReuseCursor;
    private final NewRawCursor newRawCursor;

    PackedAdjacencyList(
        long[] pages,
        int[] allocationSizes,
        HugeIntArray degrees,
        HugeLongArray offsets,
        MemoryInfo memoryInfo
    ) {
        this.pages = pages;
        this.degrees = degrees;
        this.offsets = offsets;
        this.memoryInfo = memoryInfo;
        this.cleanable = CLEANER.register(this, new AdjacencyListCleaner(pages, allocationSizes));

        var adjacencyPackingStrategy = GdsFeatureToggles.ADJACENCY_PACKING_STRATEGY.get();

        switch (adjacencyPackingStrategy) {
            case VAR_LONG_TAIL:
                this.newCursor = PackedAdjacencyList::newCursorWithVarLongTail;
                this.newReuseCursor = PackedAdjacencyList::newReuseCursorWithVarLengthTail;
                this.newRawCursor = PackedAdjacencyList::newRawCursorWithVarLongTail;
                break;
            case PACKED_TAIL:
                this.newCursor = PackedAdjacencyList::newCursorWithPackedTail;
                this.newReuseCursor = PackedAdjacencyList::newReuseCursorWithPackedTail;
                this.newRawCursor = PackedAdjacencyList::newRawCursorWithPackedTail;
                break;
            case BLOCK_ALIGNED_TAIL:
                this.newCursor = PackedAdjacencyList::newCursorWithBlockAlignedTail;
                this.newReuseCursor = PackedAdjacencyList::newReuseCursorWithBlockAlignedTail;
                this.newRawCursor = PackedAdjacencyList::newRawCursorWithBlockAlignedTail;
                break;
            default:
                throw new IllegalArgumentException("Unsupported packing strategy: " + adjacencyPackingStrategy);
        }
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
        return this.newCursor.newCursor(offset, degree, this.pages);
    }

    @Override
    public AdjacencyCursor adjacencyCursor(@Nullable AdjacencyCursor reuse, long node, double fallbackValue) {
        var degree = this.degree(node);
        if (degree == 0) {
            return AdjacencyCursor.empty();
        }

        long offset = this.offsets.get(node);
        return this.newReuseCursor.newCursor(reuse, offset, degree, this.pages);
    }

    @Override
    public AdjacencyCursor rawAdjacencyCursor() {
        return this.newRawCursor.newRawCursor(this.pages);
    }

    @Override
    public MemoryInfo memoryInfo() {
        return this.memoryInfo;
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
    public void free() {
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

