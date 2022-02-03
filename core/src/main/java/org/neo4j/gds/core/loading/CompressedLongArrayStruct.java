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

import org.neo4j.gds.collections.arraylist.HugeSparseIntArrayList;
import org.neo4j.gds.collections.arraylist.HugeSparseLongArrayList;
import org.neo4j.gds.collections.arraylist.HugeSparseObjectArrayList;
import org.neo4j.gds.core.compress.AdjacencyCompressor;
import org.neo4j.gds.mem.BitUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.neo4j.gds.core.loading.AdjacencyPreAggregation.IGNORE_VALUE;
import static org.neo4j.gds.core.loading.VarLongEncoding.encodeVLongs;
import static org.neo4j.gds.core.loading.VarLongEncoding.encodedVLongSize;
import static org.neo4j.gds.core.loading.VarLongEncoding.zigZag;
import static org.neo4j.gds.core.loading.ZigZagLongDecoding.zigZagUncompress;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class CompressedLongArrayStruct {

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final long[] EMPTY_PROPERTIES = new long[0];

    private final HugeSparseObjectArrayList<byte[]> targetLists;
    private final Map<Integer, HugeSparseObjectArrayList<long[]>> weights;
    private final HugeSparseIntArrayList positions;
    private final HugeSparseLongArrayList lastValues;
    private final HugeSparseIntArrayList lengths;
    
    private final long[][] weightsBuffer; 

    public CompressedLongArrayStruct() {
        this(0);
    }

    public CompressedLongArrayStruct(int numberOfProperties) {
        this.targetLists = HugeSparseObjectArrayList.of(EMPTY_BYTES, byte[].class);
        this.positions = HugeSparseIntArrayList.of(0);
        this.lastValues = HugeSparseLongArrayList.of(0);
        this.lengths = HugeSparseIntArrayList.of(0);


        if (numberOfProperties > 0) {
            this.weights = new HashMap<>(numberOfProperties);
            for (int i = 0; i < numberOfProperties; i++) {
                this.weights.put(i, HugeSparseObjectArrayList.of(EMPTY_PROPERTIES, long[].class));
            }
        } else {
            this.weights = null;
        }

        weightsBuffer = new long[numberOfProperties][];
    }

    /**
     * For memory efficiency, we reuse the {@code values}. They cannot be reused after calling this method.
     *
     * @param values values to write
     * @param start  start index in values
     * @param end    end index in values
     */
    public void add(long index, long[] values, int start, int end, int valuesToAdd) {
        // not inlined to avoid field access
        long currentLastValue = this.lastValues.get(index);
        long delta;
        long compressedValue;
        int requiredBytes = 0;
        for (int i = start; i < end; i++) {
            if(values[i] == IGNORE_VALUE) {
                continue;
            }

            delta = values[i] - currentLastValue;
            compressedValue = zigZag(delta);
            currentLastValue = values[i];
            values[i] = compressedValue;
            requiredBytes += encodedVLongSize(compressedValue);
        }
        var position = positions.get(index);

        var compressedTargets = ensureCompressedTargetsCapacity(index, position, requiredBytes);

        var newPosition = encodeVLongs(values, start, end, compressedTargets, position);

        positions.set(index, newPosition);

        this.lastValues.set(index, currentLastValue);
        this.lengths.addTo(index, valuesToAdd);
    }

    /**
     * For memory efficiency, we reuse the {@code values}. They cannot be reused after calling this method.
     *
     * @param values        values to write
     * @param allWeights    weights to write
     * @param start         start index in values and weights
     * @param end           end index in values and weights
     * @param valuesToAdd  the actual number of targets to import from this range
     */
    public void add(long index, long[] values, long[][] allWeights, int start, int end, int valuesToAdd) {
        // write weights
        for (int i = 0; i < allWeights.length; i++) {
            long[] weights = allWeights[i];
            addWeights(index, values, weights, start, end, i, valuesToAdd);
        }

        // write values
        add(index, values, start, end, valuesToAdd);
    }

    private void addWeights(long index, long[] values, long[] weights, int start, int end, int weightIndex, int weightsToAdd) {
        var length = lengths.get(index);
        
        var currentWeights = ensurePropertyCapacity(index, length, weightsToAdd, weightIndex);
        
        if (weightsToAdd == end - start) {
            System.arraycopy(weights, start, currentWeights, length, weightsToAdd);
        } else {
            var writePos = length;
            for (int i = 0; i < (end - start); i++) {
                if (values[start + i] != IGNORE_VALUE) {
                    currentWeights[writePos++] = weights[start + i];
                }
            }
        }
    }

    private byte[] ensureCompressedTargetsCapacity(long index, int pos, int required) {
        int targetLength = pos + required;
        var compressedTargets = targetLists.get(index);

        if (targetLength < 0) {
            throw new IllegalArgumentException(formatWithLocale(
                "Encountered numeric overflow in internal buffer. Was at position %d and needed to grow by %d.",
                pos,
                required
            ));
        } else if (compressedTargets.length <= targetLength) {
            int newLength = BitUtil.nextHighestPowerOfTwo(targetLength);
            compressedTargets = Arrays.copyOf(compressedTargets, newLength);
            this.targetLists.set(index, compressedTargets);
        }

        return compressedTargets;
    }

    private long[] ensurePropertyCapacity(long index, int pos, int required, int weightIndex) {
        int targetLength = pos + required;
        
        var currentWeights = weights.get(weightIndex).get(index);
        
        if (targetLength < 0) {
            throw new IllegalArgumentException(formatWithLocale(
                "Encountered numeric overflow in internal buffer. Was at position %d and needed to grow by %d.",
                pos,
                required
            ));
        } else if (currentWeights.length <= pos + required) {
            int newLength = BitUtil.nextHighestPowerOfTwo(pos + required);
            currentWeights = Arrays.copyOf(currentWeights, newLength);
            weights.get(weightIndex).set(index, currentWeights);
        }
        
        return currentWeights;
    }

    public int length(long index) {
        return lengths.get(index);
    }

    public int uncompress(long index, long[] into, AdjacencyCompressor.ValueMapper mapper) {
        assert into.length >= lengths.get(index);
        return zigZagUncompress(targetLists.get(index), positions.get(index), into, mapper);
    }

    public byte[] storage(long index) {
        return targetLists.get(index);
    }

    public long[][] weights(long index) {
        weights.forEach((propertyIndex, weightList) -> weightsBuffer[propertyIndex] = weightList.get(index));
        return weightsBuffer;
    }

    public boolean hasWeights() {
        return weights != null && !(weights.isEmpty());
    }

    public void release() {

    }
}
