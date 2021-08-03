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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.graphalgo.core.loading.DoubleCodec;
import org.neo4j.graphalgo.core.loading.DoubleCodec.CompressionInfo;
import org.neo4j.util.FeatureToggles;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Locale;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static java.util.function.Predicate.not;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class DoubleCodecTestBase {

    private static final boolean DEBUG_PRINT = FeatureToggles.flag(DoubleCodecTestBase.class, "debugPrint", false);
    private static final int NUMBER_OF_DOUBLES_TO_TEST = 100_000;

    private final DoubleCodec compressor;

    public DoubleCodecTestBase(DoubleCodec compressor) {
        this.compressor = compressor;
    }

    static double longAsDouble(long value) {
        long signBit = value & (1L << 63);
        if (value < 0) {
            value = -(value + 1);
        }
        assert value < 1L << 51 : "Can only encode longs with an absolute value of less than " + (1L << 51);
        long dataBits = value & ((1L << 51) - 1);
        long doubleBits = Double.doubleToLongBits(Double.NaN) | signBit | dataBits;
        return Double.longBitsToDouble(doubleBits);
    }

    static long longFromDouble(double value) {
        long doubleBits = Double.doubleToRawLongBits(value);
        long longValue = doubleBits & ((1L << 51) - 1);
        long signBit = doubleBits & (1L << 63);
        if (signBit != 0) {
            longValue = -longValue - 1;
        }
        return longValue;
    }

    @ValueSource(longs = {0, 1, 42, 1337, -0, -1, -42, -1337, Integer.MAX_VALUE, Integer.MIN_VALUE, 1L << 50, -(1L << 50)})
    @ParameterizedTest
    void testLongEmbedding(long value) {
        double encoded = longAsDouble(value);
        assertTrue(Double.isNaN(encoded));
        long decoded = longFromDouble(encoded);
        assertEquals(value, decoded);
    }

    @ParameterizedTest
    @MethodSource("testDoubles")
    void testSingleValueDoubleCompression(double input) {
        var compressed = compressor.compressDouble(input);
        var info = compressor.describeCompressedValue(compressed, 0, input);
        new Check(info).execute();
    }

    @Test
    void testDoubleCompression() {
        testDoubleCompression(testDoubles(), true);
    }

    @Test
    void testDoubleCompressionForConsecutiveIdValues() {
        var data = LongStream.range(0, NUMBER_OF_DOUBLES_TO_TEST).mapToDouble(v -> (double) v).toArray();
        testDoubleCompression(data, false);
    }

    @Test
    void testDoubleCompressionForRandomValues() {
        var seed = new Random().nextLong();
        var random = new Random(seed);
        // Note, this whole spiel of generating doubles could be as simple as
        //     var data = random.doubles(NUMBER_OF_DOUBLES_TO_TEST, 0.0, 0x1p53).toArray();
        // But that produces 98% worst-case input cases
        var maxSignificantWidth = compressor.supportedSignificandWith();
        var data = DoubleStream.generate(() -> {
            var significantWidth = random.nextInt(maxSignificantWidth);
            var significant = Long.reverse(random.nextLong() & ((1L << significantWidth) - 1));
            var exponent = random.nextInt(2047);
            return Double.longBitsToDouble(((long) exponent) << 52 | significant);
        }).limit(NUMBER_OF_DOUBLES_TO_TEST).toArray();
        try {
            testDoubleCompression(data, false);
        } catch (AssertionError e) {
            throw new RuntimeException("Seed for random values: " + seed, e);
        }
    }

    static double[] testDoubles() {
        var specialNaN = Double.longBitsToDouble(Double.doubleToLongBits(Double.NaN) + 42);
        return new double[]{
            0.0,
            +0.0,
            -0.0,
            0.15,
            -0.15,
            0.1 + 0.2,
            -0.1 - 0.2,
            1.0,
            2.0,
            3.0,
            -1.0,
            -2.0,
            -3.0,
            23.0,
            -23.0,
            23.142857,
            -23.142857,
            42.0,
            -42.0,
            127.0,
            128.0,
            129.0,
            1024.0,
            -2048.0,
            16_777_216.0,
            -1337_42_1337.0,
            1337.42,
            Math.pow(2, 52) - 1,
            Math.pow(2, 52),
            Math.pow(2, 52) + 1,
            Math.pow(2, 53) - 1,
            Math.pow(2, 53),
            Math.pow(2, 53) + 1,
            Math.pow(2, 53) + 2,
            Math.pow(2, 54),
            Math.pow(2, 54) + 1,
            Math.pow(2, 54) + 2,
            Math.pow(2, 55),
            Math.pow(2, 55) + 1,
            Math.pow(2, 55) + 2,
            Math.pow(2, 56),
            Math.pow(2, 56) + 1,
            Math.pow(2, 56) + 2,
            Math.pow(2, 255),
            Math.pow(2, 256),
            Math.pow(2, 512),
            Math.pow(2, 512) + 42,
            Double.POSITIVE_INFINITY,
            Double.NEGATIVE_INFINITY,
            Double.NaN,
            specialNaN
        };
    }

    void testDoubleCompression(double[] original, boolean detailPrint) {
        // given input from original

        // when compressing
        var compressed = new byte[10 * original.length];
        var outLength = compressor.compressDoubles(original, original.length, compressed);
        compressed = Arrays.copyOf(compressed, outLength);

        // and then decompressing
        RuntimeException caughtException = null;
        var decompressed = new double[original.length];
        try {
            compressor.decompressDoubles(compressed, original.length, decompressed, 0);
        } catch (RuntimeException e) {
            caughtException = e;
        }

        // compare compression ratio
        var uncompressedBuffer = ByteBuffer.allocate(Long.BYTES * original.length).order(ByteOrder.BIG_ENDIAN);
        for (var v : original) {
            uncompressedBuffer.putLong(Double.doubleToRawLongBits(v));
        }
        var uncompressed = Arrays.copyOfRange(
            uncompressedBuffer.array(),
            uncompressedBuffer.arrayOffset(),
            uncompressedBuffer.arrayOffset() + uncompressedBuffer.position()
        );
        var uncompressedSize = uncompressed.length;
        var compressedSize = compressed.length;
        var savings = 1 - (double) compressedSize / (double) uncompressedSize;
        var bytesPerValue = (double) compressedSize / (double) original.length;

        var compressionInfos = new ArrayList<CompressionInfo>(original.length);
        var sizeDistribution = new int[10];
        var typeDistribution = new int[9];
        var sizePerType = new int[9][10];
        var pos = 0;
        for (double input : original) {
            var info = compressor.describeCompressedValue(compressed, pos, input);
            compressionInfos.add(info);
            var size = info.compressedSize();
            var type = info.compressedType();
            sizeDistribution[size]++;
            typeDistribution[type]++;
            sizePerType[type][size]++;
            pos += size;
        }

        debugPrint(
            original,
            detailPrint,
            compressed,
            decompressed,
            uncompressed,
            uncompressedSize,
            compressedSize,
            savings,
            bytesPerValue,
            sizeDistribution,
            typeDistribution,
            sizePerType
        );

        // then all values are identical
        assertAll(
            "Check compression<>decompression cycle",
            compressionInfos
                .stream()
                .map(Check::new)
                .filter(not(Check::valid))
                .limit(5)
                .map(Check::asExecutable)
        );

        if (caughtException != null) {
            throw caughtException;
        }
    }

    @SuppressForbidden(reason = "this is supposed to print helpful stuff")
    private void debugPrint(
        double[] original,
        boolean detailPrint,
        byte[] compressed,
        double[] decompressed,
        byte[] uncompressed,
        int uncompressedSize,
        int compressedSize,
        double savings,
        double bytesPerValue,
        int[] sizeDistribution,
        int[] typeDistribution,
        int[][] sizePerType
    ) {
        if (DEBUG_PRINT) {
            if (detailPrint) {
                System.out.printf(Locale.ENGLISH, "original = %s%n", Arrays.toString(original));
                System.out.printf(Locale.ENGLISH, "decompressed = %s%n", Arrays.toString(decompressed));
                System.out.printf(Locale.ENGLISH, "compressed [%d] = %s%n", compressedSize, Arrays.toString(compressed));
                System.out.printf(Locale.ENGLISH, "uncompressed [%d] = %s%n",
                    uncompressedSize, Arrays.toString(uncompressed));
                System.out.printf(Locale.ENGLISH, "space savings %.2f%%%n", 100 * savings);
                System.out.printf(Locale.ENGLISH, "bytes per value %.4f%n", bytesPerValue);
            } else {
                System.out.printf(
                    Locale.ENGLISH,
                    "uncompressed size = [%d] | compressed size = [%d] | space savings %.2f%% | bytes per value = %.4f%n",
                    uncompressedSize,
                    compressedSize,
                    100 * savings,
                    bytesPerValue
                );
            }

            System.out.println("   Compression size  |  Number of Values  |  Percentile  |  Total Percentile");
            var cumulativeSizes = 0;
            for (var compressionSize = 0; compressionSize < sizeDistribution.length; compressionSize++) {
                var numberOfValuesAtCompression = sizeDistribution[compressionSize];
                cumulativeSizes += numberOfValuesAtCompression;
                System.out.printf(
                    Locale.ENGLISH,
                    "  %17s  |  %16s  |  %9.2f%%  |  %15.2f%%%n",
                    compressionSize,
                    numberOfValuesAtCompression,
                    100 * (double) numberOfValuesAtCompression / original.length,
                    100 * (double) cumulativeSizes / original.length
                );
            }

            System.out.println();
            System.out.println();
            System.out.println(
                "   Compression type  |  Compression size  |  Number of Values  |  Percentile  |  Total Percentile");
            var cumulativeTypes = 0;
            for (var compressionType = 0; compressionType < typeDistribution.length; compressionType++) {
                var numberOfValuesWithCompressionType = typeDistribution[compressionType];
                cumulativeTypes += numberOfValuesWithCompressionType;
                if (numberOfValuesWithCompressionType > 0) {
                    System.out.printf(
                        Locale.ENGLISH,
                        "  %17s  |  %16s  |  %16s  |  %9.2f%%  |  %15.2f%%%n",
                        compressor.describeCompression(compressionType),
                        "",
                        numberOfValuesWithCompressionType,
                        100 * (double) numberOfValuesWithCompressionType / original.length,
                        100 * (double) cumulativeTypes / original.length
                    );
                    var sizes = sizePerType[compressionType];
                    var allSizes = 0;
                    for (var compressionSize = 0; compressionSize < sizes.length; compressionSize++) {
                        var numberOfValuesAtCompression = sizes[compressionSize];
                        allSizes += numberOfValuesAtCompression;
                        if (numberOfValuesAtCompression > 0) {
                            System.out.printf(
                                Locale.ENGLISH,
                                "  %17s  |  %16s  |  %16s  |  %7.2f%% ¦  |  %13.2f%% ¦%n",
                                "",
                                compressionSize,
                                numberOfValuesAtCompression,
                                100 * (double) numberOfValuesAtCompression / numberOfValuesWithCompressionType,
                                100 * (double) allSizes / numberOfValuesWithCompressionType
                            );
                        }
                    }
                }
            }
        }
    }

    private static final class Check implements Executable {
        private final CompressionInfo info;

        private Check(CompressionInfo info) {
            this.info = info;
        }

        boolean valid() {
            return Double.compare(info.input(), info.decompressed()) == 0;
        }

        Executable asExecutable() {
            return this;
        }

        @Override
        public void execute() {
            var expected = info.input();
            var actual = info.decompressed();
            assertEquals(expected, actual, 1e-6, () -> String.format(
                Locale.ENGLISH,
                "Expected value compressed as [%3$s]%4$s to be equal to input [%2$f] (%2$A) but it actually was decompressed as [%1$f] (%1$A)",
                actual,
                expected,
                info.compressionDescription(),
                Arrays.toString(info.compressed())
            ));
        }
    }
}
