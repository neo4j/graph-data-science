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
package org.neo4j.gds.msbfs;

final class SourceNodes implements BfsSources {
    private final long[] sourceNodes;
    private final int maxPos;
    private final int startPos;
    private final long offset;
    private long sourceMask;
    private int pos;

    SourceNodes(long[] sourceNodes) {
        this.sourceNodes = sourceNodes;
        this.maxPos = sourceNodes.length;
        this.offset = 0L;
        this.startPos = -1;
    }

    SourceNodes(long offset, int length) {
        this.sourceNodes = null;
        this.maxPos = length;
        this.offset = offset;
        this.startPos = -1;
    }

    void reset() {
        this.pos = startPos;
        fetchNext();
    }

    void reset(long sourceMask) {
        assert sourceMask != 0;
        this.sourceMask = sourceMask;
        reset();
    }

    @Override
    public boolean hasNext() {
        return pos < maxPos;
    }

    @Override
    public long nextLong() {
        int current = this.pos;
        fetchNext();
        return sourceNodes != null ? sourceNodes[current] : (long) current + offset;
    }

    @Override
    public int size() {
        // reset() _always_ calls into fetchNext() which
        // finds the right-most set bit, updates pos to
        // its position and flips it. The correct size()
        // is therefore the number of set bits + 1.
        // Note, that this is under the assumption that
        // the source mask is never 0 on reset.
        return Long.bitCount(sourceMask) + 1;
    }

    private void fetchNext() {
        pos = Long.numberOfTrailingZeros(sourceMask);
        sourceMask ^= Long.lowestOneBit(sourceMask);
    }
}
