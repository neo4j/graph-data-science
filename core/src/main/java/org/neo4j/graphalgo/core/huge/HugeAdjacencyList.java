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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.graphalgo.core.huge.loader.MutableIntValue;
import org.neo4j.graphalgo.core.utils.paged.MemoryUsage;

import static org.neo4j.graphalgo.core.utils.paged.PageUtil.indexInPage;
import static org.neo4j.graphalgo.core.utils.paged.PageUtil.pageIndex;

public final class HugeAdjacencyList {

    public static final int PAGE_SHIFT = 18;
    public static final int PAGE_SIZE = 262144; // 1 << PAGE_SHIFT
    public static final long PAGE_MASK = 262143L; // PAGE_SIZE - 1

    private final long allocatedMemory;
    private byte[][] pages;

    public HugeAdjacencyList(byte[][] pages) {
        this.pages = pages;
        this.allocatedMemory = memoryOfPages(pages);
    }

    private static long memoryOfPages(byte[][] pages) {
        long memory = MemoryUsage.sizeOfObjectArray(pages.length);
        for (byte[] page : pages) {
            if (page != null) {
                memory += MemoryUsage.sizeOfByteArray(page.length);
            }
        }
        return memory;
    }

    int getDegree(long index) {
        return AdjacencyDecompression.readInt(
                pages[pageIndex(index, PAGE_SHIFT)],
                indexInPage(index, PAGE_MASK));
    }

    Cursor newCursor() {
        return new Cursor(pages);
    }

    public final long release() {
        if (pages == null) {
            return 0L;
        }
        pages = null;
        return allocatedMemory;
    }

    Cursor deltaCursor(Cursor reuse, long offset) {
        return reuse.init(offset);
    }

    public static final class Cursor extends MutableIntValue {

        // TODO: free
        private byte[][] pages;
        private final AdjacencyDecompression decompress;

        private int maxTargets;
        private int currentTarget;

        private Cursor(byte[][] pages) {
            this.pages = pages;
            this.decompress = new AdjacencyDecompression();
        }

        /**
         * Copy iteration state from another cursor without changing {@code other}.
         */
        void copyFrom(Cursor other) {
            decompress.copyFrom(other.decompress);
            currentTarget = other.currentTarget;
            maxTargets = other.maxTargets;
        }

        /**
         * Return how many targets can be decoded in total. This is equivalent to the degree.
         */
        public int cost() {
            return maxTargets;
        }

        /**
         * Return how many targets are still left to be decoded.
         */
        int remaining() {
            return maxTargets - currentTarget;
        }

        /**
         * Return true iff there is at least one more target to decode.
         */
        boolean hasNextVLong() {
            return currentTarget < maxTargets;
        }

        /**
         * Read and decode the next target id.
         * It is undefined behavior if this is called after {@link #hasNextVLong()} returns {@code false}.
         */
        long nextVLong() {
            int current = currentTarget++;
            int remaining = maxTargets - current;
            return decompress.next(remaining);
        }

        /**
         * Read and decode target ids until it is strictly larger than (`>`) the provided {@code target}.
         * Might return an id that is less than or equal to {@code target} iff the cursor did exhaust before finding an
         * id that is large enough.
         * {@code skipUntil(target) <= target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
         * will return {@code false}
         */
        long skipUntil(long target) {
            long value = decompress.skipUntil(target, remaining(), this);
            this.currentTarget += this.value;
            return value;
        }

        /**
         * Read and decode target ids until it is larger than or equal (`>=`) the provided {@code target}.
         * Might return an id that is less than {@code target} iff the cursor did exhaust before finding an
         * id that is large enough.
         * {@code advance(target) < target} can be used to distinguish the no-more-ids case and afterwards {@link #hasNextVLong()}
         * will return {@code false}
         */
        long advance(long target) {
            long value = decompress.advance(target, remaining(), this);
            this.currentTarget += this.value;
            return value;
        }

        Cursor init(long fromIndex) {
            maxTargets = decompress.reset(
                    pages[pageIndex(fromIndex, PAGE_SHIFT)],
                    indexInPage(fromIndex, PAGE_MASK));
            currentTarget = 0;
            return this;
        }
    }
}
