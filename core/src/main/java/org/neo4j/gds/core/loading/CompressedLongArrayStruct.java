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

import org.neo4j.gds.collections.DrainingIterator;
import org.neo4j.gds.collections.arraylist.HugeSparseIntArrayList;
import org.neo4j.gds.collections.arraylist.HugeSparseLongArrayList;
import org.neo4j.gds.collections.arraylist.HugeSparseObjectArrayList;
import org.neo4j.gds.mem.BitUtil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.loading.AdjacencyPreAggregation.IGNORE_VALUE;
import static org.neo4j.gds.core.loading.VarLongEncoding.encodeVLongs;
import static org.neo4j.gds.core.loading.VarLongEncoding.encodedVLongSize;
import static org.neo4j.gds.core.loading.VarLongEncoding.zigZag;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class CompressedLongArrayStruct {

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final long[] EMPTY_PROPERTIES = new long[0];

    private final HugeSparseObjectArrayList<byte[]> targetLists;
    private final Map<Integer, HugeSparseObjectArrayList<long[]>> properties;
    private final HugeSparseIntArrayList positions;
    private final HugeSparseLongArrayList lastValues;
    private final HugeSparseIntArrayList lengths;

    public CompressedLongArrayStruct() {
        this(0);
    }

    CompressedLongArrayStruct(int numberOfProperties) {
        this.targetLists = HugeSparseObjectArrayList.of(EMPTY_BYTES, byte[].class);
        this.positions = HugeSparseIntArrayList.of(0);
        this.lastValues = HugeSparseLongArrayList.of(0);
        this.lengths = HugeSparseIntArrayList.of(0);


        if (numberOfProperties > 0) {
            this.properties = new HashMap<>(numberOfProperties);
            for (int i = 0; i < numberOfProperties; i++) {
                this.properties.put(i, HugeSparseObjectArrayList.of(EMPTY_PROPERTIES, long[].class));
            }
        } else {
            this.properties = null;
        }
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
            if (values[i] == IGNORE_VALUE) {
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
     * @param values      values to write
     * @param allProperties  properties to write
     * @param start       start index in values and properties
     * @param end         end index in values and properties
     * @param valuesToAdd the actual number of targets to import from this range
     */
    public void add(long index, long[] values, long[][] allProperties, int start, int end, int valuesToAdd) {
        // write properties
        for (int i = 0; i < allProperties.length; i++) {
            long[] properties = allProperties[i];
            addProperties(index, values, properties, start, end, i, valuesToAdd);
        }

        // write values
        add(index, values, start, end, valuesToAdd);
    }

    private void addProperties(
        long index,
        long[] values,
        long[] properties,
        int start,
        int end,
        int weightIndex,
        int propertiesToAdd
    ) {
        var length = lengths.get(index);

        var currentProperties = ensurePropertyCapacity(index, length, propertiesToAdd, weightIndex);

        if (propertiesToAdd == end - start) {
            System.arraycopy(properties, start, currentProperties, length, propertiesToAdd);
        } else {
            var writePos = length;
            for (int i = 0; i < (end - start); i++) {
                if (values[start + i] != IGNORE_VALUE) {
                    currentProperties[writePos++] = properties[start + i];
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

        var currentProperties = properties.get(weightIndex).get(index);

        if (targetLength < 0) {
            throw new IllegalArgumentException(formatWithLocale(
                "Encountered numeric overflow in internal buffer. Was at position %d and needed to grow by %d.",
                pos,
                required
            ));
        } else if (currentProperties.length <= pos + required) {
            int newLength = BitUtil.nextHighestPowerOfTwo(pos + required);
            currentProperties = Arrays.copyOf(currentProperties, newLength);
            properties.get(weightIndex).set(index, currentProperties);
        }

        return currentProperties;
    }

    public long capacity() {
        return targetLists.capacity();
    }

    public boolean contains(long index) {
        return targetLists.contains(index);
    }

    public void consume(Consumer consumer) {
        new CompositeDrainingIterator(targetLists, properties, positions, lastValues, lengths).consume(consumer);
    }

    public void release() {

    }

    public interface Consumer {
        void accept(long sourceId, byte[] targets, long[][] properties, int compressedByteSize, int numberOfCompressedTargets);
    }

    private static class CompositeDrainingIterator {
        private final DrainingIterator<byte[][]> targetListIterator;
        private final DrainingIterator.DrainingBatch<byte[][]> targetListBatch;
        private final DrainingIterator<int[]> positionsListIterator;
        private final DrainingIterator.DrainingBatch<int[]> positionsListBatch;
        private final DrainingIterator<long[]> lastValuesListIterator;
        private final DrainingIterator.DrainingBatch<long[]> lastValuesListBatch;
        private final DrainingIterator<int[]> lengthsListIterator;
        private final DrainingIterator.DrainingBatch<int[]> lengthsListBatch;
        private final List<DrainingIterator<long[][]>> propertyIterators;
        private final List<DrainingIterator.DrainingBatch<long[][]>> propertyBatches;

        private final long[][] propertiesBuffer;

        CompositeDrainingIterator(
            HugeSparseObjectArrayList<byte[]> targets,
            Map<Integer, HugeSparseObjectArrayList<long[]>> properties,
            HugeSparseIntArrayList positions,
            HugeSparseLongArrayList lastValues,
            HugeSparseIntArrayList lengths
        ) {
            this.targetListIterator = targets.drainingIterator();
            this.targetListBatch = targetListIterator.drainingBatch();
            this.positionsListIterator = positions.drainingIterator();
            this.positionsListBatch = positionsListIterator.drainingBatch();
            this.lastValuesListIterator = lastValues.drainingIterator();
            this.lastValuesListBatch = lastValuesListIterator.drainingBatch();
            this.lengthsListIterator = lengths.drainingIterator();
            this.lengthsListBatch = lengthsListIterator.drainingBatch();

            if (properties == null) {
                propertyIterators = List.of();
                propertyBatches = List.of();
                propertiesBuffer = null;
            } else {
                this.propertyIterators = properties
                    .values()
                    .stream()
                    .map(HugeSparseObjectArrayList::drainingIterator)
                    .collect(Collectors.toList());
                this.propertyBatches = propertyIterators
                    .stream()
                    .map(DrainingIterator::drainingBatch)
                    .collect(Collectors.toList());
                propertiesBuffer = new long[properties.size()][];
            }
        }

        public void consume(Consumer consumer) {
            while (targetListIterator.next(targetListBatch)) {
                positionsListIterator.next(positionsListBatch);
                lastValuesListIterator.next(lastValuesListBatch);
                lengthsListIterator.next(lengthsListBatch);
                for (int i = 0; i < propertyIterators.size(); i++) {
                    propertyIterators.get(i).next(propertyBatches.get(i));
                }

                var targetsPage = targetListBatch.page;
                var positionsPage = positionsListBatch.page;
                var lengthsPage = lengthsListBatch.page;

                var offset = targetListBatch.offset;

                for (int indexInPage = 0; indexInPage < targetsPage.length; indexInPage++) {
                    var targets = targetsPage[indexInPage];
                    if (targets == EMPTY_BYTES) {
                        continue;
                    }
                    var position = positionsPage[indexInPage];
                    var length = lengthsPage[indexInPage];
                    for (int propertyIndex = 0; propertyIndex < propertyBatches.size(); propertyIndex++) {
                        propertiesBuffer[propertyIndex] = propertyBatches.get(propertyIndex).page[indexInPage];
                    }

                    consumer.accept(offset + indexInPage, targets, propertiesBuffer, position, length);
                }
            }
        }
    }
}
