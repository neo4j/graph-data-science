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

import org.neo4j.gds.core.compress.AdjacencyCompressor;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.Arrays;

import static org.neo4j.gds.core.loading.AdjacencyPreAggregation.IGNORE_VALUE;
import static org.neo4j.gds.core.loading.VarLongEncoding.encodeVLongs;
import static org.neo4j.gds.core.loading.VarLongEncoding.encodedVLongSize;
import static org.neo4j.gds.core.loading.VarLongEncoding.zigZag;
import static org.neo4j.gds.core.loading.ZigZagLongDecoding.zigZagUncompress;
import static org.neo4j.gds.mem.BitUtil.ceilDiv;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class CompressedLongArray {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private byte[] compressedTargets;
    private long[][] properties;
    private int pos;
    private long lastValue;
    private int length;

    public static MemoryEstimation memoryEstimation(long avgDegree, long nodeCount, int propertyCount) {
        // Best case scenario:
        // Difference between node identifiers in each adjacency list is 1.
        // This leads to ideal compression through delta encoding.
        int deltaBestCase = 1;
        long bestCaseCompressedTargetsSize = compressedTargetSize(avgDegree, nodeCount, deltaBestCase);

        // Worst case scenario:
        // Relationships are equally distributed across nodes, i.e. each node has the same number of rels.
        // Within each adjacency list, all identifiers have the highest possible difference between each other.
        // Highest possible difference is the number of nodes divided by the average degree.
        long deltaWorstCase = (avgDegree > 0) ? ceilDiv(nodeCount, avgDegree) : 0L;
        long worstCaseCompressedTargetsSize = compressedTargetSize(avgDegree, nodeCount, deltaWorstCase);

        return MemoryEstimations.builder(CompressedLongArray.class)
            .fixed(
                "compressed targets",
                MemoryRange.of(bestCaseCompressedTargetsSize, worstCaseCompressedTargetsSize)
            )
            .fixed("properties", MemoryUsage.sizeOfObjectArray(propertyCount) + propertyCount * MemoryUsage.sizeOfLongArray(avgDegree))
            .build();
    }

    private static long compressedTargetSize(long avgDegree, long nodeCount, long delta) {
        long firstAdjacencyIdAvgByteSize = (avgDegree > 0) ? ceilDiv(encodedVLongSize(nodeCount), 2) : 0L;
        int relationshipByteSize = encodedVLongSize(delta);
        long compressedAdjacencyByteSize = relationshipByteSize * Math.max(0, (avgDegree - 1));
        return MemoryUsage.sizeOfByteArray(firstAdjacencyIdAvgByteSize + compressedAdjacencyByteSize);
    }

    public CompressedLongArray() {
        this(0);
    }

    public CompressedLongArray(int numberOfProperties) {
        compressedTargets = EMPTY_BYTES;
        if (numberOfProperties > 0) {
            properties = new long[numberOfProperties][0];
        }
    }

    /**
     * For memory efficiency, we reuse the {@code values}. They cannot be reused after calling this method.
     *
     * @param values values to write
     * @param start  start index in values
     * @param end    end index in values
     */
    public void add(long[] values, int start, int end, int valuesToAdd) {
        // not inlined to avoid field access
        long currentLastValue = this.lastValue;
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
        ensureCapacity(this.pos, requiredBytes, this.compressedTargets);
        this.pos = encodeVLongs(values, start, end, this.compressedTargets, this.pos);

        this.lastValue = currentLastValue;
        this.length += valuesToAdd;
    }

    /**
     * For memory efficiency, we reuse the {@code values}. They cannot be reused after calling this method.
     *
     * @param values        values to write
     * @param allProperties properties to write
     * @param start         start index in values and properties
     * @param end           end index in values and properties
     * @param valuesToAdd   the actual number of targets to import from this range
     */
    public void add(long[] values, long[][] allProperties, int start, int end, int valuesToAdd) {
        // write properties
        for (int i = 0; i < allProperties.length; i++) {
            long[] properties = allProperties[i];
            addProperties(values, properties, start, end, i, valuesToAdd);
        }

        // write values
        add(values, start, end, valuesToAdd);
    }

    private void addProperties(long[] values, long[] properties, int start, int end, int propertyIndex, int propertiesToAdd) {
        ensureCapacity(length, propertiesToAdd, propertyIndex);

        if (propertiesToAdd == end - start) {
            System.arraycopy(properties, start, this.properties[propertyIndex], this.length, propertiesToAdd);
        } else {
            var writePos = length;
            for (int i = 0; i < (end - start); i++) {
                if (values[start + i] != IGNORE_VALUE) {
                    this.properties[propertyIndex][writePos++] = properties[start + i];
                }
            }
        }
    }

    void ensureCapacity(int pos, int required, byte[] storage) {
        int targetLength = pos + required;
        if (targetLength < 0) {
            throw new IllegalArgumentException(formatWithLocale(
                "Encountered numeric overflow in internal buffer. Was at position %d and needed to grow by %d.",
                pos,
                required
            ));
        } else if (storage.length <= targetLength) {
            int newLength = BitUtil.nextHighestPowerOfTwo(targetLength);
            this.compressedTargets = Arrays.copyOf(storage, newLength);
        }
    }

    private void ensureCapacity(int pos, int required, int propertyIndex) {
        int targetLength = pos + required;
        if (targetLength < 0) {
            throw new IllegalArgumentException(formatWithLocale(
                "Encountered numeric overflow in internal buffer. Was at position %d and needed to grow by %d.",
                pos,
                required
            ));
        } else if (properties[propertyIndex].length <= pos + required) {
            int newLength = BitUtil.nextHighestPowerOfTwo(pos + required);
            properties[propertyIndex] = Arrays.copyOf(properties[propertyIndex], newLength);
        }
    }

    public int length() {
        return length;
    }

    public int uncompress(long[] into, AdjacencyCompressor.ValueMapper mapper) {
        assert into.length >= length;
        return zigZagUncompress(compressedTargets, pos, into, mapper);
    }

    public byte[] storage() {
        return compressedTargets;
    }

    public long[][] properties() {
        return properties;
    }

    public boolean hasProperties() {
        return properties != null && !(properties.length == 0);
    }

    public void release() {
        compressedTargets = null;
        properties = null;
        pos = 0;
        length = 0;
    }
}
