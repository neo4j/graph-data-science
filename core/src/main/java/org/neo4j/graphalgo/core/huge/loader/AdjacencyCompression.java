/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.core.huge.loader;

import com.carrotsearch.hppc.sorting.IndirectComparator;
import com.carrotsearch.hppc.sorting.IndirectSort;
import org.apache.lucene.util.LongsRef;
import org.neo4j.graphalgo.core.DuplicateRelationshipsStrategy;

import java.util.Arrays;

import static org.neo4j.graphalgo.core.huge.loader.VarLongEncoding.encodeVLongs;

public final class AdjacencyCompression {

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

    static int applyDeltaEncoding(LongsRef data) {
        Arrays.sort(data.longs, 0, data.length);
        return data.length = applyDelta(data.longs, data.length);
    }

    // TODO: requires lots of additional memory ... inline indirect sort to make reuse of - to be created - buffers
    static int applyDeltaEncoding(LongsRef data, long[] weights, DuplicateRelationshipsStrategy duplicateRelationshipsStrategy) {
        int[] order = IndirectSort.mergesort(0, data.length, new AscendingLongComparator(data.longs));

        long[] sortedValues = new long[data.length];
        long[] sortedWeights = new long[data.length];

        data.length = applyDelta(order, data.longs, sortedValues, weights, sortedWeights, data.length, duplicateRelationshipsStrategy);

        System.arraycopy(sortedValues, 0, data.longs, 0, data.length);
        System.arraycopy(sortedWeights, 0, weights, 0, data.length);

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

    private static int applyDelta(long[] values, int length) {
        long value = values[0], delta;
        int in = 1, out = 1;
        for (; in < length; ++in) {
            delta = values[in] - value;
            value = values[in];
            // only keep the relationship if we don't already have
            // one that points to the same target node
            // no support for #parallel-edges
            if (delta > 0L) {
                values[out++] = delta;
            }
        }
        return out;
    }

    public static int counter = 0;
    /**
     * Applies delta encoding to the given {@code values}.
     * Weights are not encoded, {@code outWeights} contains weights according to {@code order}.
     */
    private static int applyDelta(
            int[] order,
            long[] values,
            long[] outValues,
            long[] weights,
            long[] outWeights,
            int length,
            DuplicateRelationshipsStrategy duplicateRelationshipsStrategy) {
        int firstSortIdx = order[0];
        long value = values[firstSortIdx];
        long delta;

        outValues[0] = values[firstSortIdx];
        outWeights[0] = weights[firstSortIdx];

        int in = 1, out = 1;
        for (; in < length; ++in) {
            final int sortIdx = order[in];
            delta = values[sortIdx] - value;
            value = values[sortIdx];

            // only keep the relationship if we don't already have
            // one that points to the same target node
            // no support for #parallel-edges
            // if delta > 0L then the relationship is to a new node
            if (delta > 0L) {
                outWeights[out] = weights[sortIdx];
                outValues[out++] = delta;
            } else {
                int existingIdx = out - 1;
                double existingWeight = Double.longBitsToDouble(outWeights[existingIdx]);
                double newWeight = Double.longBitsToDouble(weights[sortIdx]);
                newWeight = duplicateRelationshipsStrategy.merge(existingWeight, newWeight);
                outWeights[existingIdx] = Double.doubleToLongBits(newWeight);
            }
        }
        return out;
    }

    private AdjacencyCompression() {
    }

    private static class AscendingLongComparator implements IndirectComparator {
        private final long[] array;

        AscendingLongComparator(long[] array) {
            this.array = array;
        }

        public int compare(int indexA, int indexB) {
            long a = this.array[indexA];
            long b = this.array[indexB];
            if (a < b) {
                return -1;
            } else {
                return a > b ? 1 : 0;
            }
        }
    }
}
