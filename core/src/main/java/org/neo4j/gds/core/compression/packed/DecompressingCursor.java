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

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.collections.PageUtil;
import org.neo4j.gds.core.compression.common.BumpAllocator;

public final class DecompressingCursor implements AdjacencyCursor {

    private final long[] pages;

    private final BlockDecompressor decompressingReader;

    private int maxTargets;
    private int currentPosition;

    public DecompressingCursor(long[] pages) {
        this.pages = pages;
        this.decompressingReader = new BlockDecompressor();
    }

    @Override
    public void init(long offset, int degree) {
        int pageIndex = PageUtil.pageIndex(offset, BumpAllocator.PAGE_SHIFT);
        int idxInPage = PageUtil.indexInPage(offset, BumpAllocator.PAGE_MASK);

        long pagePtr = this.pages[pageIndex];
        if (pagePtr == 0) {
            throw new IllegalStateException("This page has already been freed.");
        }
        long listPtr = pagePtr + idxInPage;
        this.maxTargets = degree;
        this.currentPosition = 0;
        this.decompressingReader.reset(listPtr, degree);
    }

    @Override
    public int size() {
        return this.maxTargets;
    }

    @Override
    public int remaining() {
        return this.maxTargets - this.currentPosition;
    }

    @Override
    public boolean hasNextVLong() {
        return currentPosition < maxTargets;
    }

    @Override
    public long nextVLong() {
        this.currentPosition++;
        return decompressingReader.next();
    }

    @Override
    public long peekVLong() {
        return decompressingReader.peek();
    }

    @Override
    public long skipUntil(long targetId) {
        long next;
        while (hasNextVLong()) {
            if ((next = nextVLong()) > targetId) {
                return next;
            }
        }
        return AdjacencyCursor.NOT_FOUND;
    }

    @Override
    public long advance(long targetId) {
        long next;
        while (hasNextVLong()) {
            if ((next = nextVLong()) >= targetId) {
                return next;
            }
        }
        return AdjacencyCursor.NOT_FOUND;
    }

    @Override
    public long advanceBy(int n) {
        throw new UnsupportedOperationException("not yet implemented");
    }

    @Override
    public @NotNull AdjacencyCursor shallowCopy(@Nullable AdjacencyCursor destination) {
        throw new UnsupportedOperationException("not yet implemented");
    }
}
