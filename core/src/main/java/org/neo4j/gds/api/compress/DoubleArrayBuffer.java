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
import org.neo4j.gds.core.compression.packed.AdjacencyPacking;
import org.neo4j.gds.mem.BitUtil;

public final class DoubleArrayBuffer {

    private static final double[] EMPTY_BUFFER = new double[0];

    public double[] buffer;
    public int length;

    public DoubleArrayBuffer() {
        this.buffer = EMPTY_BUFFER;
        this.length = 0;
    }

    @TestOnly
    public DoubleArrayBuffer(double[] buffer, int length) {
        this.buffer = buffer;
        this.length = length;
    }

    /**
     * Make sure to be able to hold at least {@code length} elements.
     * Throws existing data away.
     */
    public void ensureCapacity(int length) {
        int alignedLength = (int) BitUtil.align(length, AdjacencyPacking.BLOCK_SIZE);
        if (this.buffer.length < alignedLength) {
            this.buffer = new double[alignedLength];
        }
    }
}
