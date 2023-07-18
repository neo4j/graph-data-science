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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junitpioneer.jupiter.params.IntRangeSource;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.memory.EmptyMemoryTracker;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

class AdjacencyPackingTest {

    @ParameterizedTest
    @IntRangeSource(from = 0, to = 64, closed = true)
    void loopPackingRoundtripTest(int bits) {
        long upperBound = (1L << bits) - 1;
        long[] data = LongStream.concat(LongStream.range(0, Math.min(42, upperBound)), LongStream.of(upperBound))
            .toArray();
        int length = data.length;
        int requiredBytes = BitUtil.ceilDiv(length * bits, Byte.SIZE);

        long allocation = BitUtil.align(requiredBytes, Long.BYTES);
        long ptr = UnsafeUtil.allocateMemory(allocation, EmptyMemoryTracker.INSTANCE);

        AdjacencyPacking.loopPack(bits, data, 0, length, ptr);

        long[] unpacked = new long[(int) BitUtil.align(length, AdjacencyPacking.BLOCK_SIZE)];
        AdjacencyUnpacking.loopUnpack(bits, unpacked, 0, length, ptr);

        assertThat(Arrays.copyOf(unpacked, length)).isEqualTo(data);

        UnsafeUtil.free(ptr, allocation, EmptyMemoryTracker.INSTANCE);
    }

    @Test
    void loopPack0Test() {
        long allocation = BitUtil.align(1, Long.BYTES);
        long ptr = UnsafeUtil.allocateMemory(allocation, EmptyMemoryTracker.INSTANCE);
        long next = AdjacencyPacking.loopPack(0, new long[0], 0, 0, ptr);
        assertThat(next).isEqualTo(ptr);

        UnsafeUtil.free(ptr, allocation, EmptyMemoryTracker.INSTANCE);
    }

    @Test
    void loopPack5Test() {
        int bits = 5;
        long upperBound = (1L << bits) - 1;
        long[] data = LongStream.rangeClosed(0, upperBound).toArray();
        int length = data.length;
        int requiredBytes = BitUtil.ceilDiv(length * bits, Byte.SIZE);

        long allocation = BitUtil.align(requiredBytes, Long.BYTES);
        long ptr = UnsafeUtil.allocateMemory(allocation, EmptyMemoryTracker.INSTANCE);
        long next = AdjacencyPacking.loopPack(5, data, 0, length, ptr);

        var w0 = UnsafeUtil.getLong(ptr);
        assertThat(w0).isEqualTo(0b1100_01011_01010_01001_01000_00111_00110_00101_00100_00011_00010_00001_00000L);
        var w1 = UnsafeUtil.getLong(ptr + Long.BYTES);
        assertThat(w1).isEqualTo(0b001_11000_10111_10110_10101_10100_10011_10010_10001_10000_01111_01110_01101_0L);
        var w2 = UnsafeUtil.getLong(ptr + Long.BYTES + Long.BYTES);
        assertThat(w2).isEqualTo(0b11111_11110_11101_11100_11011_11010_11L);

        assertThat(next).isEqualTo(ptr + allocation);

        UnsafeUtil.free(ptr, allocation, EmptyMemoryTracker.INSTANCE);
    }

    @Test
    void loopPack64Test() {
        int bits = 64;
        long upperBound = -1L;
        long[] data = LongStream.concat(LongStream.rangeClosed(0, 42), LongStream.of(upperBound)).toArray();
        int length = data.length;
        int requiredBytes = BitUtil.ceilDiv(length * bits, Byte.SIZE);

        long allocation = BitUtil.align(requiredBytes, Long.BYTES);
        long ptr = UnsafeUtil.allocateMemory(allocation, EmptyMemoryTracker.INSTANCE);
        long next = AdjacencyPacking.loopPack(bits, data, 0, length, ptr);

        var w0 = UnsafeUtil.getLong(ptr);
        assertThat(w0).isEqualTo(0L);
        var w1 = UnsafeUtil.getLong(ptr + Long.BYTES);
        assertThat(w1).isEqualTo(1L);
        var w2 = UnsafeUtil.getLong(ptr + Long.BYTES + Long.BYTES);
        assertThat(w2).isEqualTo(2);
        var w42 = UnsafeUtil.getLong(ptr + Long.BYTES * 42);
        assertThat(w42).isEqualTo(42);
        var wUpperBound = UnsafeUtil.getLong(ptr + Long.BYTES * 43);
        assertThat(wUpperBound).isEqualTo(upperBound);

        assertThat(next).isEqualTo(ptr + allocation + Long.BYTES);

        UnsafeUtil.free(ptr, allocation, EmptyMemoryTracker.INSTANCE);
    }
}
