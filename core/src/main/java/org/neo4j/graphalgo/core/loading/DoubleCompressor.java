/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.graphalgo.annotation.ValueClass;

import java.util.Arrays;

public abstract class DoubleCompressor {

    /**
     * The number of logical bits in the significand of a
     * {@code double} number, including the implicit bit.
     */
    static final int SIGNIFICAND_WIDTH = 53;

    /**
     * The number of physical bits in the significand of a
     * {@code double} number.
     */
    protected static final int SIGNIFICAND_BITS = SIGNIFICAND_WIDTH - 1;

    /**
     * The number of physical bits in the exponent of a
     * {@code double} number.
     */
    static final int EXPONENT_BITS = 11;

    /**
     * Bias used in representing a {@code double} exponent.
     */
    protected static final int EXP_BIAS = 1023;

    /**
     * Exponent in representing a {@code NaN} or {@code Infinity} value.
     */
    protected static final int SUPER_NORMAL_EXPONENT = (1 << EXPONENT_BITS) - 1;

    /**
     * Bit mask to isolate the sign bit of a {@code double}.
     */
    static final long SIGN_BIT_MASK = 0x8000000000000000L;

    /**
     * Bit mask to isolate the exponent field of a
     * {@code double}.
     */
    protected static final long EXP_BIT_MASK = 0x7FF0000000000000L;

    /**
     * Bit mask to isolate the significand field of a
     * {@code double}.
     */
    protected static final long SIGNIFICANT_BIT_MASK = 0x000FFFFFFFFFFFFFL;

    /**
     * Get the sign bit of a bit representation of a double.
     */
    protected static byte getSign(long bits) {
        var signBit = (bits & SIGN_BIT_MASK) >>> (SIGNIFICAND_BITS + EXPONENT_BITS);
        return (byte) signBit;
    }

    /**
     * Get the unbiased exponent of a bit representation of a double.
     */
    protected static int getUnbiasedExponent(long bits) {
        return (int) ((bits & EXP_BIT_MASK) >>> SIGNIFICAND_BITS);
    }

    /**
     * Get the significand of a bit representation of a double.
     */
    protected static long getSignificand(long bits) {
        return bits & SIGNIFICANT_BIT_MASK;
    }

    public int compressDoubles(double[] data, int length, byte[] out) {
        var outPos = 0;
        length = Math.min(data.length, length);
        for (var i = 0; i < length; i++) {
            var bits = Double.doubleToRawLongBits(data[i]);
            outPos = compressDouble(bits, out, outPos);
        }
        return outPos;
    }

    int compressDoubles(long[] data, int length, byte[] out) {
        var outPos = 0;
        length = Math.min(data.length, length);
        for (var i = 0; i < length; i++) {
            var datum = data[i];
            outPos = compressDouble(datum, out, outPos);
        }
        return outPos;
    }

    byte[] compressDoubles(double[] data) {
        var out = new byte[10 * data.length];
        var outPos = 0;
        for (var datum : data) {
            var bytes = compressDouble(datum);
            System.arraycopy(bytes, 0, out, outPos, bytes.length);
            outPos += bytes.length;
        }
        return Arrays.copyOf(out, outPos);
    }

    byte[] compressDouble(double value) {
        var out = new byte[10];
        var outLength = compressDouble(value, out);
        return Arrays.copyOf(out, outLength);
    }

    public int compressDouble(double value, byte[] out) {
        var bits = Double.doubleToRawLongBits(value);
        return compressDouble(bits, out, 0);
    }

    double decompressDouble(byte[] data) {
        return decompressDouble(data, 0);
    }

    public double[] decompressDoubles(byte[] data, int length) {
        return decompressDoubles(data, 0, length);
    }

    double[] decompressDoubles(byte[] data, int pos, int length) {
        var out = new double[length];
        decompressDoubles(data, pos, length, out, 0);
        return out;
    }

    int decompressDoubles(byte[] data, int length, double[] out, int outPos) {
        return decompressDoubles(data, 0, length, out, outPos);
    }

    int decompressDoubles(byte[] data, int inPos, int length, double[] out, int outPos) {
        var value = new MutableDouble();
        for (var i = 0; i < length; i++) {
            inPos = decompressDouble(data, inPos, value);
            out[outPos++] = value.doubleValue();
        }
        return outPos;
    }

    /**
     * Compress the double from its bit representation and write result into out.
     *
     * @param doubleBits the double value as converted by {@link Double#doubleToRawLongBits(double)}.
     * @param out where to write the compressed value to.
     * @param outPos at which position to write the compressed value in {@code out}.
     * @return the new value of {@code outPos} (NOT the number of bytes written).
     */
    public abstract int compressDouble(long doubleBits, byte[] out, int outPos);

    /**
     * Decompress a single double from the given byte array and write the result into out.
     *
     * @param data the compressed data.
     * @param pos start reading from {@code data} at this position.
     * @param out output value, the result should be written using {@link MutableDouble#doubleValue()}.
     * @return the new value of {@code pos} after reading the compressed value.
     */
    public abstract int decompressDouble(byte[] data, int pos, MutableDouble out);

    /**
     * Decompress a single double from the given byte array and return it.
     * There is no way to know how many bytes have been read.
     *
     * @param data the compressed data.
     * @param pos start reading from {@code data} at this position.
     * @return the decompressed value.
     */
    public double decompressDouble(byte[] data, int pos) {
        var out = new MutableDouble();
        decompressDouble(data, pos, out);
        return out.doubleValue();
    }

    /**
     * Return how many into how many bytes the current value is compressed.
     *
     * @param data the compressed data.
     * @param pos start reading from {@code data} at this position.
     * @return the number of bytes that the compressed value at {@code pos} is.
     */
    public abstract int compressedSize(byte[] data, int pos);

    /**
     * Return some String description on how the data is compressed.
     * For debugging or testing.
     *
     * @param type a type identifier.
     * @return some string for describing how the data at {@code pos} is compressed.
     */
    @TestOnly
    public abstract String describeCompression(int type);

    /**
     * Return debug info about how the current value is compressed.
     * For debugging or testing.
     *
     * @param data the compressed data.
     * @param pos start reading from {@code data} at this position.
     * @return info object describing the current compressed value.
     */
    @TestOnly
    public abstract CompressionInfo describeCompressedValue(byte[] data, int pos, double originalInput);

    @TestOnly
    public int supportedSignificandWith() {
        return SIGNIFICAND_WIDTH;
    }

    @ValueClass
    @TestOnly
    public interface CompressionInfo {
        double input();

        byte[] compressed();

        double decompressed();

        int compressedSize();

        int compressedType();

        String compressionDescription();
    }
}
