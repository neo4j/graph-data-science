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

import com.carrotsearch.hppc.sorting.IndirectSort;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.compress.AdjacencyCompressor;
import org.neo4j.gds.core.compress.LongArrayBuffer;
import org.neo4j.gds.core.utils.AscendingLongComparator;

import java.util.Arrays;

import static org.neo4j.gds.core.huge.VarLongDecoding.decodeDeltaVLongs;
import static org.neo4j.gds.core.loading.VarLongEncoding.encodeVLongs;
import static org.neo4j.gds.core.loading.VarLongEncoding.encodedVLongsSize;
import static org.neo4j.gds.core.loading.ZigZagLongDecoding.zigZagUncompress;

public final class AdjacencyCompression {

    /**
     * Decompress the given {@code array} into the given {@code into}.
     * After this, {@link org.neo4j.gds.core.compress.LongArrayBuffer#length} will reflect the number of decompressed values
     * that are in the {@link org.neo4j.gds.core.compress.LongArrayBuffer#buffer}.
     */
    public static void copyFrom(LongArrayBuffer into, byte[] targets, int compressedValues, int limit, AdjacencyCompressor.ValueMapper mapper) {
        into.ensureCapacity(compressedValues);
        copyFrom(into.buffer, targets, compressedValues, limit, mapper);
        into.length = compressedValues;
    }

    public static void copyFrom(long[] into, byte[] targets, int compressedValues, int limit, AdjacencyCompressor.ValueMapper mapper) {
        assert into.length >= compressedValues;
        zigZagUncompress(targets, limit, into, mapper);
    }

    public static int applyDeltaEncoding(LongArrayBuffer data, Aggregation aggregation) {
        return data.length = applyDeltaEncoding(data.buffer, data.length, aggregation);
    }

    public static int applyDeltaEncoding(long[] data, int length, Aggregation aggregation) {
        Arrays.sort(data, 0, length);
        return deltaEncodeSortedValues(data, 0, length, aggregation);
    }

    // TODO: requires lots of additional memory ... inline indirect sort to make reuse of - to be created - buffers
    static int applyDeltaEncoding(LongArrayBuffer data, long[][] weights, Aggregation[] aggregations, boolean noAggregation) {
        return data.length = applyDeltaEncoding(data.buffer, data.length, weights, aggregations, noAggregation);
    }

    // TODO: requires lots of additional memory ... inline indirect sort to make reuse of - to be created - buffers
    static int applyDeltaEncoding(long[] data, int length, long[][] weights, Aggregation[] aggregations, boolean noAggregation) {
        int[] order = IndirectSort.mergesort(0, length, new AscendingLongComparator(data));

        long[] sortedValues = new long[length];
        long[][] sortedWeights = new long[weights.length][length];

        length = applyDelta(
                order,
                data,
                sortedValues,
                weights,
                sortedWeights,
                length,
                aggregations,
                noAggregation
        );

        System.arraycopy(sortedValues, 0, data, 0, length);
        for (int i = 0; i < sortedWeights.length; i++) {
            long[] sortedWeight = sortedWeights[i];
            System.arraycopy(sortedWeight, 0, weights[i], 0, length);
        }

        return length;
    }

    static byte[] ensureBufferSize(LongArrayBuffer data, byte[] out) {
        return ensureBufferSize(data.buffer, out, data.length);
    }

    static byte[] ensureBufferSize(long[] data, byte[] out, int length) {
        var requiredBytes = encodedVLongsSize(data, length);
        if (requiredBytes > out.length) {
            return new byte[requiredBytes];
        }
        return out;
    }

    public static int compress(LongArrayBuffer data, byte[] out) {
        return compress(data.buffer, out, data.length);
    }

    public static int compress(long[] data, byte[] out, int length) {
        return encodeVLongs(data, length, out, 0);
    }

    public static int compress(long[] data, int offset, int length, byte[] out) {
        return encodeVLongs(data, offset, offset + length, out, 0);
    }

    public static byte[] deltaEncodeAndCompress(long[] values, int offset, int length, Aggregation aggregation) {
        length = AdjacencyCompression.deltaEncodeSortedValues(values, offset, length, aggregation);
        var requiredBytes = VarLongEncoding.encodedVLongsSize(values, offset, length);
        var compressed = new byte[requiredBytes];
        compress(values, offset, length, compressed);
        return compressed;
    }

    public static long[] decompress(byte[] compressed, int numberOfValues) {
        long[] out = new long[numberOfValues];
        decodeDeltaVLongs(0L, compressed, 0, numberOfValues, out);
        return out;
    }

    public static int deltaEncodeSortedValues(long[] values, int offset, int length, Aggregation aggregation) {
        long value = values[offset], delta;
        int end = offset + length;
        int in = offset + 1, out = in;
        for (; in < end; ++in) {
            delta = values[in] - value;
            value = values[in];
            if (delta > 0L || aggregation == Aggregation.NONE) {
                values[out++] = delta;
            }
        }
        return out;
    }

    public static void prefixSumDeltaEncodedValues(long[] values, int length) {
        length = Math.min(values.length, length);
        long value = values[0];
        for (int idx = 1; idx < length; ++idx) {
            value = values[idx] += value;
        }
    }

    /**
     * Applies delta encoding to the given {@code values}.
     * Weights are not encoded.
     *
     * @param order Ordered indices into {@code values} and {@code weights} for consuming these in ascending value order.
     * @param values Relationships represented by target node ID.
     * @param outValues Sorted, delta-encoded and optionally aggregated relationships.
     * @param weights Relationship properties by key, ordered by {@code order}.
     * @param outWeights Sorted and optionally aggregated relationship properties.
     * @param length Number of relationships (degree of source node) to process.
     * @param aggregations Aggregations to apply to parallel edges. One per relationship property key in {@code weights}.
     * @param noAggregation Is true iff all aggregations are NONE.
     */
    private static int applyDelta(
            int[] order,
            long[] values,
            long[] outValues,
            long[][] weights,
            long[][] outWeights,
            int length,
            Aggregation[] aggregations,
            boolean noAggregation
    ) {
        int firstSortIdx = order[0];
        long value = values[firstSortIdx];
        long delta;

        outValues[0] = values[firstSortIdx];
        for (int i = 0; i < weights.length; i++) {
            outWeights[i][0] = weights[i][firstSortIdx];
        }

        int in = 1, out = 1;
        for (; in < length; ++in) {
            final int sortIdx = order[in];
            delta = values[sortIdx] - value;
            value = values[sortIdx];

            if (delta > 0L || noAggregation) {
                for (int i = 0; i < weights.length; i++) {
                    outWeights[i][out] = weights[i][sortIdx];
                }
                outValues[out++] = delta;
            } else {
                for (int i = 0; i < weights.length; i++) {
                    Aggregation aggregation = aggregations[i];
                    int existingIdx = out - 1;
                    long[] outWeight = outWeights[i];
                    double existingWeight = Double.longBitsToDouble(outWeight[existingIdx]);
                    double newWeight = Double.longBitsToDouble(weights[i][sortIdx]);
                    newWeight = aggregation.merge(existingWeight, newWeight);
                    outWeight[existingIdx] = Double.doubleToLongBits(newWeight);
                }
            }
        }
        return out;
    }

    private AdjacencyCompression() {
    }
}
