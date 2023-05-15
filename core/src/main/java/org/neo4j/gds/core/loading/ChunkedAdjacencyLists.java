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
import org.neo4j.gds.collections.HugeSparseByteArrayArrayList;
import org.neo4j.gds.collections.HugeSparseCollections;
import org.neo4j.gds.collections.HugeSparseIntList;
import org.neo4j.gds.collections.HugeSparseLongArrayList;
import org.neo4j.gds.collections.HugeSparseLongList;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.mem.MemoryUsage;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.core.compression.common.VarLongEncoding.encodeVLongs;
import static org.neo4j.gds.core.compression.common.VarLongEncoding.encodedVLongSize;
import static org.neo4j.gds.core.compression.common.VarLongEncoding.zigZag;
import static org.neo4j.gds.core.loading.AdjacencyPreAggregation.IGNORE_VALUE;
import static org.neo4j.gds.mem.BitUtil.ceilDiv;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class ChunkedAdjacencyLists {

    private static final byte[] EMPTY_BYTES = new byte[0];
    private static final byte[][] EMPTY_BYTES_BYTES = new byte[0][];
    private static final long[] EMPTY_PROPERTIES = new long[0];

    private final HugeSparseByteArrayArrayList targetLists;
    private final HugeSparseLongArrayList[] properties;
    private final HugeSparseIntList positions;
    private final HugeSparseLongList lastValues;
    private final HugeSparseIntList lengths;

    private final byte[] compressBuffer = new byte[1 << 20]; // 1MiB

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

        return MemoryEstimations.builder(ChunkedAdjacencyLists.class)
            .fixed("compressed targets", MemoryRange.of(bestCaseCompressedTargetsSize, worstCaseCompressedTargetsSize))
            .fixed("positions", HugeSparseCollections.estimateInt(nodeCount, nodeCount))
            .fixed("lengths", HugeSparseCollections.estimateInt(nodeCount, nodeCount))
            .fixed("lastValues", HugeSparseCollections.estimateLong(nodeCount, nodeCount))
            .fixed("properties", HugeSparseCollections.estimateLongArray(nodeCount, nodeCount, (int) avgDegree).times(propertyCount))
            .build();
    }

    private static long compressedTargetSize(long avgDegree, long nodeCount, long delta) {
        long firstAdjacencyIdAvgByteSize = (avgDegree > 0) ? ceilDiv(encodedVLongSize(nodeCount), 2) : 0L;
        int relationshipByteSize = encodedVLongSize(delta);
        long compressedAdjacencyByteSize = relationshipByteSize * Math.max(0, (avgDegree - 1));
        return nodeCount * MemoryUsage.sizeOfByteArray(firstAdjacencyIdAvgByteSize + compressedAdjacencyByteSize);
    }

    public static ChunkedAdjacencyLists of(int numberOfProperties, long initialCapacity) {
        return new ChunkedAdjacencyLists(numberOfProperties, initialCapacity);
    }

    private ChunkedAdjacencyLists(int numberOfProperties, long initialCapacity) {
        this.targetLists = HugeSparseByteArrayArrayList.of(EMPTY_BYTES_BYTES, initialCapacity);
        this.positions = HugeSparseIntList.of(0, initialCapacity);
        this.lastValues = HugeSparseLongList.of(0, initialCapacity);
        this.lengths = HugeSparseIntList.of(0, initialCapacity);


        if (numberOfProperties > 0) {
            this.properties = new HugeSparseLongArrayList[numberOfProperties];
            Arrays.setAll(this.properties, i -> HugeSparseLongArrayList.of(EMPTY_PROPERTIES, initialCapacity));
        } else {
            this.properties = null;
        }
    }

    /**
     * For memory efficiency, we reuse the {@code values}. They cannot be reused after calling this method.
     *
     * @param targets values to write
     * @param start   start index in values
     * @param end     end index in values
     */
    public void add(long index, long[] targets, int start, int end, int valuesToAdd) {
        // not inlined to avoid field access
        long currentLastValue = this.lastValues.get(index);
        long delta;
        long compressedValue;
        int requiredBytes = 0;
        for (int i = start; i < end; i++) {
            if (targets[i] == IGNORE_VALUE) {
                continue;
            }
            delta = targets[i] - currentLastValue;
            compressedValue = zigZag(delta);
            currentLastValue = targets[i];
            targets[i] = compressedValue;
            requiredBytes += encodedVLongSize(compressedValue);
        }
        var position = positions.get(index);
        encodeVLongs(targets, start, end, this.compressBuffer, 0);

        copyCompressedBytes(index, position, this.compressBuffer, requiredBytes);

        positions.set(index, position + requiredBytes);

        this.lastValues.set(index, currentLastValue);
        this.lengths.addTo(index, valuesToAdd);
    }

    /**
     * For memory efficiency, we reuse the {@code values}. They cannot be reused after calling this method.
     *
     * @param targets       values to write
     * @param allProperties properties to write
     * @param start         start index in values and properties
     * @param end           end index in values and properties
     * @param targetsToAdd  the actual number of targets to import from this range
     */
    public void add(long index, long[] targets, long[][] allProperties, int start, int end, int targetsToAdd) {
        // write properties
        for (int i = 0; i < allProperties.length; i++) {
            addProperties(index, targets, allProperties[i], start, end, i, targetsToAdd);
        }

        // write values
        add(index, targets, start, end, targetsToAdd);
    }

    private void addProperties(
        long index,
        long[] targets,
        long[] properties,
        int start,
        int end,
        int propertyIndex,
        int propertiesToAdd
    ) {
        var length = lengths.get(index);

        var currentProperties = ensurePropertyCapacity(index, length, propertiesToAdd, propertyIndex);

        if (propertiesToAdd == end - start) {
            System.arraycopy(properties, start, currentProperties, length, propertiesToAdd);
        } else {
            var writePos = length;
            for (int i = 0; i < (end - start); i++) {
                if (targets[start + i] != IGNORE_VALUE) {
                    currentProperties[writePos++] = properties[start + i];
                }
            }
        }
    }

    private static final int[] NEXT_CHUNK_LENGTH = {
        64,
        256,
        512,
        1024,
        1024,
        2048,
        4096,
        4096,
        8192,
        8192,
        16384,
        16384,
        32768,
        32768,
        65536,
        65536,
        131072,
        131072,
        262144,
        262144,
        524288,
        524288,
        1048576,
        1048576,
    };

    private void copyCompressedBytes(long index, int targetPos, byte[] buffer, int bufferLength) {
        byte[][] compressedTargets = targetLists.get(index);

        // last chunk, write pos = posInLastChunk
        int posInLastChunk = targetPos;
        for (int targetPage = 0; targetPage < compressedTargets.length - 1; targetPage++) {
            byte[] compressedTarget = compressedTargets[targetPage];
            posInLastChunk -= compressedTarget.length;
        }

        // can we fit everything in the last chunk?
        if (compressedTargets.length > 0) {
            byte[] lastChunk = compressedTargets[compressedTargets.length - 1];
            if (lastChunk.length >= posInLastChunk + bufferLength) {
                // fits in last chunk
                System.arraycopy(buffer, 0, lastChunk, posInLastChunk, bufferLength);
                return;
            }
        }

        // how many bytes do we need to write into the last chunk
        int fitsInLastChunk = compressedTargets.length == 0
            ? 0
            : compressedTargets[compressedTargets.length - 1].length - posInLastChunk;

        int newChunkLengthAtLeast = bufferLength - fitsInLastChunk;

        // the next chunk size grows slowly depending on the number of chunks we already have
        int nextChunkLevel = Math.min(compressedTargets.length, NEXT_CHUNK_LENGTH.length - 1);
        int nextChunkLength = NEXT_CHUNK_LENGTH[nextChunkLevel];

        // avoid splitting the buffer into too many chunks
        int newChunkLength = Math.max(newChunkLengthAtLeast, nextChunkLength);

        // copy buffer into the last chunk and the new chunk
        byte[] newChunk = new byte[newChunkLength];
        if (fitsInLastChunk > 0) {
            System.arraycopy(
                buffer,
                0,
                compressedTargets[compressedTargets.length - 1],
                posInLastChunk,
                fitsInLastChunk
            );
        }
        System.arraycopy(buffer, fitsInLastChunk, newChunk, 0, bufferLength - fitsInLastChunk);

        // add new chunk to the targets list
        compressedTargets = Arrays.copyOf(compressedTargets, compressedTargets.length + 1);
        compressedTargets[compressedTargets.length - 1] = newChunk;
        targetLists.set(index, compressedTargets);
    }

    private long[] ensurePropertyCapacity(long index, int pos, int required, int propertyIndex) {
        int targetLength = pos + required;

        var currentProperties = this.properties[propertyIndex].get(index);

        if (targetLength < 0) {
            throw new IllegalArgumentException(formatWithLocale(
                "Encountered numeric overflow in internal buffer. Was at position %d and needed to grow by %d.",
                pos,
                required
            ));
        } else if (currentProperties.length <= pos + required) {
//            int newLength = ArrayUtil.oversize(pos + required, Long.BYTES);
            int newLength = BitUtil.nextHighestPowerOfTwo(pos + required);
            currentProperties = Arrays.copyOf(currentProperties, newLength);
            this.properties[propertyIndex].set(index, currentProperties);
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

    public interface Consumer {
        void accept(
            long sourceId,
            byte[][] targets,
            long[][] properties,
            int compressedByteSize,
            int numberOfCompressedTargets
        );
    }

    private static class CompositeDrainingIterator {
        private final DrainingIterator<byte[][][]> targetListIterator;
        private final DrainingIterator.DrainingBatch<byte[][][]> targetListBatch;
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
            HugeSparseByteArrayArrayList targets,
            HugeSparseLongArrayList[] properties,
            HugeSparseIntList positions,
            HugeSparseLongList lastValues,
            HugeSparseIntList lengths
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
                this.propertyIterators = Arrays.stream(properties)
                    .map(HugeSparseLongArrayList::drainingIterator)
                    .collect(Collectors.toList());
                this.propertyBatches = this.propertyIterators
                    .stream()
                    .map(DrainingIterator::drainingBatch)
                    .collect(Collectors.toList());
                propertiesBuffer = new long[properties.length][];
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
                    if (targets == EMPTY_BYTES_BYTES) {
                        continue;
                    }
                    var position = positionsPage[indexInPage];
                    var length = lengthsPage[indexInPage];
                    for (int propertyIndex = 0; propertyIndex < propertyBatches.size(); propertyIndex++) {
                        var page = propertyBatches.get(propertyIndex).page;
                        propertiesBuffer[propertyIndex] = page[indexInPage];
                        // make properties eligible for GC
                        page[indexInPage] = null;
                    }
                    // make targets eligible for GC
                    targetsPage[indexInPage] = null;

                    consumer.accept(offset + indexInPage, targets, propertiesBuffer, position, length);
                }
            }
        }
    }
}
