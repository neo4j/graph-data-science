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
package org.neo4j.graphalgo.core.loading;

import org.apache.commons.lang3.mutable.MutableDouble;
import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.annotation.ValueClass;

import java.util.Arrays;

/**
 * A class for compressing and decompressing {@code double}s.
 *
 * There are various methods for compressing either a single double, or multiple ones in one call.
 * Similar methods exist for decompression.
 * The compression itself is identical for all those methods.
 * The difference they provide is in how much reuse of buffer data structures is allowed.
 *
 * Implementors need to implement only {@link #compressDouble(long,byte[],int)} for compressing
 * and {@link #decompressDouble(byte[],int,MutableDouble)} for decompressing. The other methods
 * for compression or decompression can be override if a better implementation can be provided.
 *
 * In addition, {@link #compressedSize(byte[], int)} must provide the number of bytes for a certain compressed value.
 * This is required as many decompressing methods to not offer feedback on how much data they've read.
 *
 * For testing and debug purposes, implementors need to provide {@link #describeCompressedValue(byte[], int, double)}.
 * The value that is returned from {@link DoubleCodec.CompressionInfo#compressedType()}
 * is used to call {@link #describeCompression(int)} so that one should be implemented accordingly.
 */
public abstract class DoubleCodec {

    /**
     * The number of logical bits in the significand of a
     * {@code double} number, including the implicit bit.
     */
    static final int SIGNIFICAND_WIDTH = 53;

    /**
     * The number of physical bits in the significand of a {@code double} number.
     */
    protected static final int SIGNIFICAND_BITS = SIGNIFICAND_WIDTH - 1;

    /**
     * The number of physical bits in the exponent of a {@code double} number.
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
     * Bit mask to isolate the exponent field of a {@code double}.
     */
    protected static final long EXP_BIT_MASK = 0x7FF0000000000000L;

    /**
     * Bit mask to isolate the significand field of a {@code double}.
     */
    protected static final long SIGNIFICANT_BIT_MASK = 0x000FFFFFFFFFFFFFL;

    /**
     * Get the sign bit of a bit representation of a {@code double}.
     */
    protected static byte getSign(long bits) {
        var signBit = (bits & SIGN_BIT_MASK) >>> (SIGNIFICAND_BITS + EXPONENT_BITS);
        return (byte) signBit;
    }

    /**
     * Get the unbiased exponent of a bit representation of a {@code double}.
     */
    protected static int getUnbiasedExponent(long bits) {
        return (int) ((bits & EXP_BIT_MASK) >>> SIGNIFICAND_BITS);
    }

    /**
     * Get the significand of a bit representation of a {@code double}.
     */
    protected static long getSignificand(long bits) {
        return bits & SIGNIFICANT_BIT_MASK;
    }

    /**
     * Compress many {@code double}s in one call.
     *
     * @param data the input data to compress.
     * @param length how many value to compress, reading from {@code data} starting at index 0 upto index {@code length}.
     * @param out the output buffer where the compressed values are written to.
     * @return the number of bytes written into {@code out}.
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code out} is too small.
     */
    public int compressDoubles(double[] data, int length, byte[] out) {
        var outPos = 0;
        length = Math.min(data.length, length);
        for (var i = 0; i < length; i++) {
            var bits = Double.doubleToRawLongBits(data[i]);
            outPos = compressDouble(bits, out, outPos);
        }
        return outPos;
    }

    /**
     * Compress many {@code double}s from their {@code long} representation in one call.
     *
     * @param data the input data to compress as converted by {@link Double#doubleToRawLongBits(double)}.
     * @param length how many value to compress, reading from {@code data} starting at index 0 upto index {@code length}.
     * @param out the output buffer where the compressed values are written to.
     * @return the number of bytes written into {@code out}.
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code out} is too small.
     */
    int compressDoubles(long[] data, int length, byte[] out) {
        var outPos = 0;
        length = Math.min(data.length, length);
        for (var i = 0; i < length; i++) {
            var datum = data[i];
            outPos = compressDouble(datum, out, outPos);
        }
        return outPos;
    }

    /**
     * Compress all provided {@code double}s in one call.
     *
     * @param data the input data to compress.
     * @return the compressed {@code double}s as a {@code byte[]}.
     * @throws java.lang.NegativeArraySizeException if {@code data} is too large to compress.
     */
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

    /**
     * Compress a single {@code double}.
     *
     * @param value the value to compress.
     * @return the compressed {@code double} as a {@code byte[]}.
     */
    byte[] compressDouble(double value) {
        var out = new byte[10];
        var outLength = compressDouble(value, out);
        return Arrays.copyOf(out, outLength);
    }

    /**
     * Compress a single {@code double}.
     *
     * @param value the value to compress.
     * @param out the output buffer where the compressed value is written to.
     * @return the number of bytes written into {@code out}.
     *         Technically, this is the next position to start writing to, but since we write to the
     *         beginning of the array, this is equal to the number of bytes written.
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code out} is too small.
     */
    public int compressDouble(double value, byte[] out) {
        var bits = Double.doubleToRawLongBits(value);
        return compressDouble(bits, out, 0);
    }

    /**
     * Decompress a single {@code double}.
     * The data is read from index 0.
     * There is no way to know how many bytes have been read.
     *
     * @param data the compressed data.
     * @return the decompressed value.
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data} does not contain a complete value.
     */
    double decompressDouble(byte[] data) {
        return decompressDouble(data, 0);
    }

    /**
     * Decompress may {@code double}s in one call.
     * The data is read from index 0.
     * There is no way to know how many bytes have been read.
     *
     * @param data the compressed data.
     * @param length how many values to decompress.
     * @return the decompressed values.
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data} did not contain enough data for all values.
     */
    public double[] decompressDoubles(byte[] data, int length) {
        return decompressDoubles(data, 0, length);
    }

    /**
     * Decompress may {@code double}s in one call.
     * The data is read from the provided index.
     * There is no way to know how many bytes have been read.
     *
     * @param data the compressed data.
     * @param pos start reading from {@code data} at this position.
     * @param length how many values to decompress.
     * @return the decompressed values.
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data} did not contain enough data for all values.
     */
    double[] decompressDoubles(byte[] data, int pos, int length) {
        var out = new double[length];
        decompressDoubles(data, pos, length, out, 0);
        return out;
    }

    /**
     * Decompress may {@code double}s in one call.
     * The data is read from index 0.
     * There is no way to know how many bytes have been read.
     *
     * @param data the compressed data.
     * @param length how many values to decompress.
     * @param out the output buffer where the compressed values are written to.
     * @param outPos at which position to start writing the decompressed values in {@code out}.
     * @return the position where the next write to {@code out} should occur (NOT the number of bytes written).
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data} did not contain enough data for all values or {@code out} is too small.
     */
    int decompressDoubles(byte[] data, int length, double[] out, int outPos) {
        return decompressDoubles(data, 0, length, out, outPos);
    }

    /**
     * Decompress may {@code double}s in one call.
     * The data is read from the provided {@code inPos}.
     * There is no way to know how many bytes have been read.
     *
     * @param data the compressed data.
     * @param inPos where to start reading the compressed data from {@code data}.
     * @param length how many values to decompress.
     * @param out the output buffer where the compressed values are written to.
     * @param outPos at which position to start writing the decompressed values in {@code out}.
     * @return the position where the next write to {@code out} should occur (NOT the number of bytes written).
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data} did not contain enough data for all values or {@code out} is too small.
     */
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
     * @param out the output buffer where the compressed value is written to.
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
     * Return the number of bytes used to compress the current value.
     *
     * @param data the compressed data.
     * @param pos start reading from {@code data} at this position.
     * @return the number of bytes that the compressed value at {@code pos} is.
     */
    public abstract int compressedSize(byte[] data, int pos);

    /**
     * Return some string description on how the data is compressed.
     * For debugging or testing.
     *
     * @param type a type identifier.
     * @return some string for describing how the data is compressed.
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

    /**
     * @return the guaranteed maximum significand width.
     * If the compression is lossless, this value must equal {@link #SIGNIFICAND_WIDTH}.
     */
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
