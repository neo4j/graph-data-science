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
package org.neo4j.gds.core.loading;

import org.apache.commons.lang3.mutable.MutableDouble;

import java.util.Arrays;

@SuppressWarnings({"PointlessBitwiseExpression", "PointlessArithmeticExpression"})
public final class NoopDoubleCodec extends DoubleCodec {

    private static final DoubleCodec INSTANCE = new NoopDoubleCodec();

    public static DoubleCodec instance() {
        return INSTANCE;
    }

    @Override
    public int compressDouble(long doubleBits, byte[] out, int outPos) {
        out[0 + outPos] = (byte) ((doubleBits >>> 56) & 0xFF);
        out[1 + outPos] = (byte) ((doubleBits >>> 48) & 0xFF);
        out[2 + outPos] = (byte) ((doubleBits >>> 40) & 0xFF);
        out[3 + outPos] = (byte) ((doubleBits >>> 32) & 0xFF);
        out[4 + outPos] = (byte) ((doubleBits >>> 24) & 0xFF);
        out[5 + outPos] = (byte) ((doubleBits >>> 16) & 0xFF);
        out[6 + outPos] = (byte) ((doubleBits >>> 8) & 0xFF);
        out[7 + outPos] = (byte) ((doubleBits >>> 0) & 0xFF);
        return 8 + outPos;
    }

    @Override
    public int decompressDouble(byte[] data, int pos, MutableDouble out) {
        long bits = (((long) (data[0 + pos] & 0xFF)) << 56) |
                    (((long) (data[1 + pos] & 0xFF)) << 48) |
                    (((long) (data[2 + pos] & 0xFF)) << 40) |
                    (((long) (data[3 + pos] & 0xFF)) << 32) |
                    (((long) (data[4 + pos] & 0xFF)) << 24) |
                    (((long) (data[5 + pos] & 0xFF)) << 16) |
                    (((long) (data[6 + pos] & 0xFF)) << 8) |
                    (((long) (data[7 + pos] & 0xFF)) << 0);
        out.setValue(Double.longBitsToDouble(bits));
        return 8 + pos;
    }

    @Override
    public int compressedSize(byte[] data, int pos) {
        return 8;
    }

    @Override
    public String describeCompression(int type) {
        return "NOOP";
    }

    @Override
    public CompressionInfo describeCompressedValue(byte[] data, int pos, double originalInput) {
        return ImmutableCompressionInfo.builder()
            .input(originalInput)
            .compressed(Arrays.copyOfRange(data, pos, 8 + pos))
            .decompressed(decompressDouble(data, pos))
            .compressedSize(8)
            .compressedType(0)
            .compressionDescription("NOOP")
            .build();
    }

    private NoopDoubleCodec() {
    }
}
