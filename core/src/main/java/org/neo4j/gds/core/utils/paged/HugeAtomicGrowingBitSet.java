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
package org.neo4j.gds.core.utils.paged;

import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.mem.HugeArrays;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

public final class HugeAtomicGrowingBitSet {

    // Each page stores 2^PAGE_SHIFT_BITS entries.
    // Word-size is 64 bit (long), which means we
    // store 2^(PAGE_SHIFT_BITS - 6) words per page.
    static final int PAGE_SHIFT_BITS = 16;
    // Number of bits per word (long).
    private static final int NUM_BITS = Long.SIZE;
    private static final int BIT_MASK = NUM_BITS - 1;

    private final int pageSize; // words per page
    private final int pageShift; // word-aligned page shift
    private final long pageMask; // word-aligned page mask

    // We need to atomically update the reference to the
    // actual pages since multiple threads try to add a
    // new page at the same time and only once must succeed.
    private final AtomicReference<Pages> pages;

    public static HugeAtomicGrowingBitSet create(long bitSize) {
        // Number of words required to represent the bit size.
        long wordSize = BitUtil.ceilDiv(bitSize, NUM_BITS);

        // Parameters for long pages representing the bits.
        int pageShift = PAGE_SHIFT_BITS - 6; // 2^6 == 64 Bits for a long
        int pageSize = 1 << pageShift;
        long pageMask = pageSize - 1;
        // We allocate in pages of fixed size, so the last page
        // might have extra space, which is fine as this is a
        // growing data structure anyway. The capacity will be
        // larger than the specified size.
        int pageCount = HugeArrays.numberOfPages(wordSize, pageShift, pageMask);

        return new HugeAtomicGrowingBitSet(pageCount, pageSize, pageShift, pageMask);
    }

    private HugeAtomicGrowingBitSet(int pageCount, int pageSize, int pageShift, long pageMask) {
        this.pageSize = pageSize;
        this.pageShift = pageShift;
        this.pageMask = pageMask;
        this.pages = new AtomicReference<>(new Pages(pageCount, pageSize));
    }

    /**
     * Sets the bit at the given index to true.
     */
    public void set(long index) {
        long longIndex = index >>> 6;
        int pageIndex = HugeArrays.pageIndex(longIndex, pageShift);
        int wordIndex = HugeArrays.indexInPage(longIndex, pageMask);
        int bitIndex = (int) (index & BIT_MASK);

        var page = getPage(pageIndex);
        long bitMask = 1L << bitIndex;

        long oldWord = page.get(wordIndex);
        while (true) {
            long newWord = oldWord | bitMask;
            if (newWord == oldWord) {
                // nothing to set
                return;
            }
            long currentWord = page.compareAndExchange(wordIndex, oldWord, newWord);
            if (currentWord == oldWord) {
                // CAS successful
                return;
            }
            // CAS unsuccessful, try again
            oldWord = currentWord;
        }
    }

    /**
     * Returns the state of the bit at the given index.
     */
    public boolean get(long index) {
        long longIndex = index >>> 6;
        int pageIndex = HugeArrays.pageIndex(longIndex, pageShift);
        int wordIndex = HugeArrays.indexInPage(longIndex, pageMask);
        int bitIndex = (int) (index & BIT_MASK);

        var page = getPage(pageIndex);
        long bitMask = 1L << bitIndex;
        return (page.get(wordIndex) & bitMask) != 0;
    }

    /**
     * Sets a bit and returns the previous value.
     * The index should be less than the BitSet size.
     */
    public boolean getAndSet(long index) {
        long longIndex = index >>> 6;
        int pageIndex = HugeArrays.pageIndex(longIndex, pageShift);
        int wordIndex = HugeArrays.indexInPage(longIndex, pageMask);
        int bitIndex = (int) (index & BIT_MASK);

        var page = getPage(pageIndex);
        long bitMask = 1L << bitIndex;

        long oldWord = page.get(wordIndex);
        while (true) {
            long newWord = oldWord | bitMask;
            if (newWord == oldWord) {
                // already set
                return true;
            }
            long currentWord = page.compareAndExchange(wordIndex, oldWord, newWord);
            if (currentWord == oldWord) {
                // CAS successful
                return false;
            }
            // CAS unsuccessful, try again
            oldWord = currentWord;
        }
    }

    /**
     * Returns the number of set bits in the bit set.
     * <p>
     * The result of the method does not include the effects
     * of concurrent write operations that occur while the
     * cardinality is computed.
     */
    public long cardinality() {
        final Pages pages = this.pages.get();
        final long pageCount = pages.length();
        final long pageSize = this.pageSize;

        long setBitCount = 0;

        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
            var page = pages.getPage(pageIndex);
            for (int wordIndex = 0; wordIndex < pageSize; wordIndex++) {
                long word = page.get(wordIndex);
                setBitCount += Long.bitCount(word);
            }
        }

        return setBitCount;
    }

    /**
     * Iterates the bit set in increasing index order and calls the
     * given consumer for each index with a set bit.
     * <p>
     * The result of the method does not include the effects
     * of concurrent write operations that occur while the
     * bit set if traversed.
     */
    public void forEachSetBit(LongConsumer consumer) {
        final Pages pages = this.pages.get();
        final long pageCount = pages.length();
        final long pageSize = this.pageSize;

        long base = 0;

        for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
            var page = pages.getPage(pageIndex);
            for (int wordIndex = 0; wordIndex < pageSize; wordIndex++) {
                long word = page.get(wordIndex);

                while (word != 0) {
                    long next = Long.numberOfTrailingZeros(word);
                    consumer.accept(Long.SIZE * (base + wordIndex) + next);
                    word = word ^ Long.lowestOneBit(word);
                }
            }
            base += pageSize;
        }
    }

    /**
     * Resets the bit at the given index.
     */
    public void clear(long index) {
        long longIndex = index >>> 6;
        int pageIndex = HugeArrays.pageIndex(longIndex, pageShift);
        int wordIndex = HugeArrays.indexInPage(longIndex, pageMask);
        int bitIndex = (int) (index & BIT_MASK);

        var page = getPage(pageIndex);
        long bitMask = ~(1L << bitIndex);

        long oldWord = page.get(wordIndex);
        while (true) {
            long newWord = oldWord & bitMask;
            if (newWord == oldWord) {
                // already cleared
                return;
            }
            long currentWord = page.compareAndExchange(wordIndex, oldWord, newWord);
            if (currentWord == oldWord) {
                // CAS successful
                return;
            }
            // CAS unsuccessful, try again
            oldWord = currentWord;
        }
    }

    /**
     * The current capacity of the bit set. Setting a bit at an index
     * exceeding the capacity, leads to a resize operation.
     * The cardinality is a multiple of the underling page size:
     * 2^{@link HugeAtomicGrowingBitSet#PAGE_SHIFT_BITS}.
     */
    public long capacity() {
        return pages.get().length() * (1L << pageShift);
    }

    /**
     * Returns the page at the given index, potentially growing the underlying pages
     * to fit the requested page index.
     */
    private AtomicLongArray getPage(int pageIndex) {
        var pages = this.pages.get();

        while (pages.length() <= pageIndex) {
            // We need to grow the number of pages to fit the requested page index.
            // This needs to happen in a loop since we can't guarantee that if the
            // current thread is not successful in updating the pages, the newly
            // created pages contain enough space.
            var newPages = new Pages(pages, pageIndex + 1, this.pageSize);
            // Atomically updating the reference. If we're successful, the witness will
            // be the prior `pages` value, and we're done. If we're unsuccessful, we
            // already read the new `pages` value due to CAX call and repeat with that one.
            var witness = this.pages.compareAndExchange(pages, newPages);

            if (pages == witness) {
                // Success.
                pages = newPages;
            } else {
                // Throw away the created pages and try again with the new current value.
                pages = witness;
            }
        }

        return pages.getPage(pageIndex);
    }

    private static final class Pages {

        private final AtomicLongArray[] pages;

        private Pages(int pageCount, int pageSize) {
            var pages = new AtomicLongArray[pageCount];

            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                pages[pageIndex] = new AtomicLongArray(pageSize);
            }

            this.pages = pages;
        }

        private Pages(Pages oldPages, int newPageCount, int pageSize) {
            var pages = new AtomicLongArray[newPageCount];

            // We transfer the existing pages to the new pages.
            final int oldPageCount = oldPages.length();
            System.arraycopy(oldPages.pages, 0, pages, 0, oldPageCount);
            // And add new pages for the remaining ones until we reach the page count.
            // This is potential garbage since the thread creating those might not win
            // the race to grow the pages.
            for (int pageIndex = oldPageCount; pageIndex < newPageCount; pageIndex++) {
                pages[pageIndex] = new AtomicLongArray(pageSize);
            }

            this.pages = pages;
        }

        private AtomicLongArray getPage(int pageIndex) {
            return pages[pageIndex];
        }

        private int length() {
            return this.pages.length;
        }
    }
}
