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

import org.HdrHistogram.ConcurrentHistogram;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.ImmutableMemoryInfo;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.MemoryUsage;

import java.lang.ref.Cleaner;
import java.util.Arrays;

public class PackedAdjacencyList implements AdjacencyList {

    private static final Cleaner CLEANER = Cleaner.create();

    private final long[] pages;
    private final HugeIntArray degrees;
    private final HugeLongArray offsets;
    private final int[] allocationSizes;
    private final ConcurrentHistogram allocationHistogram;

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

    private static AdjacencyCursor newCursorWithPackedTail(long offset, int degree, long[] pages) {
        var cursor = new DecompressingCursorWithPackedTail(pages);
        cursor.init(offset, degree);
        return cursor;
    }

    private static AdjacencyCursor newReuseCursorWithPackedTail(
        @Nullable AdjacencyCursor reuse,
        long offset,
        int degree,
        long[] pages
    ) {
        if (reuse instanceof DecompressingCursorWithPackedTail) {
            reuse.init(offset, degree);
            return reuse;
        } else {
            var cursor = new DecompressingCursorWithPackedTail(pages);
            cursor.init(offset, degree);
            return cursor;
        }
    }

    private static AdjacencyCursor newRawCursorWithPackedTail(long[] pages) {
        return new DecompressingCursorWithPackedTail(pages);
    }

    private static AdjacencyCursor newCursorWithVarLongTail(long offset, int degree, long[] pages) {
        var cursor = new DecompressingCursorWithVarLongTail(pages);
        cursor.init(offset, degree);
        return cursor;
    }

    private static AdjacencyCursor newReuseCursorWithVarLongTail(
        @Nullable AdjacencyCursor reuse,
        long offset,
        int degree,
        long[] pages
    ) {
        if (reuse instanceof DecompressingCursorWithVarLongTail) {
            reuse.init(offset, degree);
            return reuse;
        } else {
            var cursor = new DecompressingCursorWithVarLongTail(pages);
            cursor.init(offset, degree);
            return cursor;
        }
    }

    private static AdjacencyCursor newRawCursorWithVarLongTail(long[] pages) {
        return new DecompressingCursorWithVarLongTail(pages);
    }

    private final NewCursor newCursor;
    private final NewReuseCursor newReuseCursor;
    private final NewRawCursor newRawCursor;

    PackedAdjacencyList(
        long[] pages,
        int[] allocationSizes,
        HugeIntArray degrees,
        HugeLongArray offsets,
        ConcurrentHistogram allocationHistogram
    ) {
        this.pages = pages;
        this.degrees = degrees;
        this.offsets = offsets;
        this.allocationSizes = allocationSizes;
        this.allocationHistogram = allocationHistogram;
        this.cleanable = CLEANER.register(this, new AdjacencyListCleaner(pages, allocationSizes));

        switch (System.getProperty("gds.compression", "packed")) {
            case "varlong":
                this.newCursor = PackedAdjacencyList::newCursorWithVarLongTail;
                this.newReuseCursor = PackedAdjacencyList::newReuseCursorWithVarLongTail;
                this.newRawCursor = PackedAdjacencyList::newRawCursorWithVarLongTail;
                break;
            case "packed":
                this.newCursor = PackedAdjacencyList::newCursorWithPackedTail;
                this.newReuseCursor = PackedAdjacencyList::newReuseCursorWithPackedTail;
                this.newRawCursor = PackedAdjacencyList::newRawCursorWithPackedTail;
                break;
            default:
                throw new IllegalArgumentException("Unknown compression type");
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
        long bytesOffHeap = Arrays.stream(this.allocationSizes).asLongStream().sum();

        var memoryInfoBuilder = ImmutableMemoryInfo
            .builder()
            .pages(this.pages.length)
            .allocationHistogram(this.allocationHistogram)
            .bytesOffHeap(bytesOffHeap);

        var bytesOnHeap = MemoryUsage.sizeOf(this);
        if (bytesOnHeap >= 0) {
            memoryInfoBuilder.bytesOnHeap(bytesOnHeap);
        }

        return memoryInfoBuilder.build();
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

