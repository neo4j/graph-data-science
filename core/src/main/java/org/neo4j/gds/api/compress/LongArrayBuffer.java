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

import org.jetbrains.annotations.TestOnly;

public final class LongArrayBuffer {

    private static final long[] EMPTY_BUFFER = new long[0];

    public long[] buffer;
    public int length;

    public LongArrayBuffer() {
        this.buffer = EMPTY_BUFFER;
        this.length = 0;
    }

    @TestOnly
    public LongArrayBuffer(long[] buffer, int length) {
        this.buffer = buffer;
        this.length = length;
    }

    /**
     * Make sure to be able to hold at least {@code length} elements.
     * Throws existing data away.
     */
    public void ensureCapacity(int length) {
        if (this.buffer.length < length) {
            // give leeway in case of nodes with a reference to themselves
            // due to automatic skipping of identical targets, just adding one is enough to cover the
            // self-reference case, as it is handled as two relationships that aren't counted by BOTH
            // avoid repeated re-allocation for smaller degrees
            // avoid generous over-allocation for larger degrees
            int newSize = Math.max(32, 1 + length);
            this.buffer = new long[newSize];
        }
    }
}
