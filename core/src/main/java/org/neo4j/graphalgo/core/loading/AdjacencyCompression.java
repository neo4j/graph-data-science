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

import com.carrotsearch.hppc.sorting.IndirectSort;
import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.utils.AscendingLongComparator;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.loading.VarLongEncoding.encodeVLongs;

final class AdjacencyCompression {

    private static long[] growWithDestroy(long[] values, int newLength) {
        if (values.length < newLength) {
            // give leeway in case of nodes with a reference to themselves
            // due to automatic skipping of identical targets, just adding one is enough to cover the
            // self-reference case, as it is handled as two relationships that aren't counted by BOTH
            // avoid repeated re-allocation for smaller degrees
            // avoid generous over-allocation for larger degrees
            int newSize = Math.max(32, 1 + newLength);
            return new long[newSize];
        }
        return values;
    }

    static void copyFrom(LongsRef into, CompressedLongArray array) {
        into.longs = growWithDestroy(into.longs, array.length());
        into.length = array.uncompress(into.longs);
    }

    static int applyDeltaEncoding(LongsRef data, Aggregation aggregation) {
        Arrays.sort(data.longs, 0, data.length);
        return data.length = applyDelta(data.longs, data.length, aggregation);
    }

    // TODO: requires lots of additional memory ... inline indirect sort to make reuse of - to be created - buffers
    static int applyDeltaEncoding(LongsRef data, long[][] weights, Aggregation[] aggregations, boolean noAggregation) {
        int[] order = IndirectSort.mergesort(0, data.length, new AscendingLongComparator(data.longs));

        long[] sortedValues = new long[data.length];
        long[][] sortedWeights = new long[weights.length][data.length];

        data.length = applyDelta(
                order,
                data.longs,
                sortedValues,
                weights,
                sortedWeights,
                data.length,
                aggregations,
                noAggregation
        );

        System.arraycopy(sortedValues, 0, data.longs, 0, data.length);
        for (int i = 0; i < sortedWeights.length; i++) {
            long[] sortedWeight = sortedWeights[i];
            System.arraycopy(sortedWeight, 0, weights[i], 0, data.length);
        }

        return data.length;
    }

    static int compress(LongsRef data, byte[] out) {
        return compress(data.longs, out, data.length);
    }

    static int compress(long[] data, byte[] out, int length) {
        return encodeVLongs(data, length, out, 0);
    }

    //@formatter:off
    static int writeDegree(byte[] out, int offset, int degree) {
        out[    offset] = (byte) (degree);
        out[1 + offset] = (byte) (degree >>> 8);
        out[2 + offset] = (byte) (degree >>> 16);
        out[3 + offset] = (byte) (degree >>> 24);
        return 4 + offset;
    }
    //@formatter:on

    private static int applyDelta(long[] values, int length, Aggregation aggregation) {
        long value = values[0], delta;
        int in = 1, out = 1;
        for (; in < length; ++in) {
            delta = values[in] - value;
            value = values[in];
            if (delta > 0L || aggregation == Aggregation.NONE) {
                values[out++] = delta;
            }
        }
        return out;
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
        boolean firstTimeSeen = true;
        for (; in < length; ++in) {
            final int sortIdx = order[in];
            delta = values[sortIdx] - value;
            value = values[sortIdx];

            if (delta > 0L || noAggregation) {
                for (int i = 0; i < weights.length; i++) {
                    outWeights[i][out] = weights[i][sortIdx];
                }
                outValues[out++] = delta;
                firstTimeSeen = true;
            } else {
                for (int i = 0; i < weights.length; i++) {
                    Aggregation aggregation = aggregations[i];
                    int existingIdx = out - 1;
                    long[] outWeight = outWeights[i];
                    double existingWeight = Double.longBitsToDouble(outWeight[existingIdx]);
                    double newWeight = Double.longBitsToDouble(weights[i][sortIdx]);
                    newWeight = aggregation.merge(firstTimeSeen, existingWeight, newWeight);
                    outWeight[existingIdx] = Double.doubleToLongBits(newWeight);
                }
                firstTimeSeen = false;
            }
        }
        return out;
    }

    private AdjacencyCompression() {
    }
}
