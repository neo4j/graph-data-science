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
package org.neo4j.gds.core.utils.partition;

import com.carrotsearch.hppc.AbstractIterator;
import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.cursor.HugeCursor;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.SetBitsIterable;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.BitUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.LongToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class PartitionUtils {

    public static final double MIN_PARTITION_CAPACITY = 0.67;
    public static final int DIVISION_FACTOR = 10;

    private PartitionUtils() {}

    public static <TASK> List<TASK> rangePartition(
        int concurrency,
        long nodeCount,
        Function<Partition, TASK> taskCreator,
        Optional<Integer> minBatchSize
    ) {
        long batchSize = ParallelUtil.adjustedBatchSize(
            nodeCount,
            concurrency,
            minBatchSize.orElse(ParallelUtil.DEFAULT_BATCH_SIZE)
        );
        return rangePartitionWithBatchSize(nodeCount, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> rangePartitionWithBatchSize(long nodeCount, long batchSize, Function<Partition, TASK> taskCreator) {
        return tasks(nodeCount, batchSize, taskCreator);
    }

    public static List<Partition> numberAlignedPartitioning(
        int concurrency,
        long nodeCount,
        long alignTo
    ) {
        return numberAlignedPartitioning(concurrency, nodeCount, alignTo, Function.identity());
    }

    public static <TASK> List<TASK> numberAlignedPartitioning(
        int concurrency,
        long nodeCount,
        long alignTo,
        Function<Partition, TASK> taskCreator
    ) {
        return numberAlignedPartitioningWithMaxSize(concurrency, nodeCount, alignTo, Long.MAX_VALUE, taskCreator);
    }

    public static List<Partition> numberAlignedPartitioningWithMaxSize(
        int concurrency,
        long nodeCount,
        long alignTo,
        long maxPartitionSize
    ) {
        return numberAlignedPartitioningWithMaxSize(
            concurrency,
            nodeCount,
            alignTo,
            maxPartitionSize,
            Function.identity()
        );
    }

    public static <TASK> List<TASK> numberAlignedPartitioningWithMaxSize(
        int concurrency,
        long nodeCount,
        long alignTo,
        long maxPartitionSize,
        Function<Partition, TASK> taskCreator
    ) {
        if (maxPartitionSize < alignTo) {
            throw new IllegalArgumentException(formatWithLocale(
                "Maximum size of a partition must be at least as much as its desired alignment but got align=%d and maxPartitionSize=%d",
                alignTo,
                maxPartitionSize
            ));
        }

        final long initialBatchSize = ParallelUtil.adjustedBatchSize(nodeCount, concurrency, alignTo);
        final long remainder = initialBatchSize % alignTo;
        long adjustedBatchSize = remainder == 0 ? initialBatchSize : initialBatchSize + (alignTo - remainder);
        if (adjustedBatchSize > maxPartitionSize) {
            long overflow = maxPartitionSize % alignTo;
            adjustedBatchSize = maxPartitionSize - overflow;
        }

        return tasks(nodeCount, adjustedBatchSize, taskCreator);
    }

    public static <TASK> List<TASK> degreePartition(
        Graph graph,
        int concurrency,
        Function<DegreePartition, TASK> taskCreator,
        Optional<Integer> minBatchSize
    ) {
        if (concurrency == 1) {
            return List.of(taskCreator.apply(new DegreePartition(0, graph.nodeCount(), graph.relationshipCount())));
        }

        var batchSize = Math.max(
            minBatchSize.orElse(ParallelUtil.DEFAULT_BATCH_SIZE),
            BitUtil.ceilDiv(graph.relationshipCount(), concurrency)
        );
        return degreePartitionWithBatchSize(graph.nodeCount(), graph::degree, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> degreePartition(
        long nodeCount,
        long relationshipCount,
        DegreeFunction degrees,
        int concurrency,
        Function<DegreePartition, TASK> taskCreator,
        Optional<Integer> minBatchSize
    ) {
        if (concurrency == 1) {
            return List.of(taskCreator.apply(new DegreePartition(0, nodeCount, relationshipCount)));
        }

        var batchSize = Math.max(
            minBatchSize.orElse(ParallelUtil.DEFAULT_BATCH_SIZE),
            BitUtil.ceilDiv(relationshipCount, concurrency)
        );
        return degreePartitionWithBatchSize(nodeCount, degrees, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> customDegreePartitionWithBatchSize(
        Graph graph,
        int concurrency,
        LongToIntFunction customDegreeFunction,
        Function<DegreePartition, TASK> taskCreator,
        Optional<Integer> minBatchSize,
        Optional<Long> weightSum
    ) {
        var actualWeightSum = weightSum.orElse(
            LongStream.range(0, graph.nodeCount()).map(customDegreeFunction::applyAsInt).sum()
        );
        var batchSize = Math.max(
            minBatchSize.orElse(ParallelUtil.DEFAULT_BATCH_SIZE),
            BitUtil.ceilDiv(actualWeightSum, concurrency)
        );
        return degreePartitionWithBatchSize(graph.nodeCount(), customDegreeFunction::applyAsInt, batchSize, taskCreator);
    }

    /**
     * Returns a stream of many small partitions (in contrast to list of few big ones)
     */
    public static Stream<DegreePartition> degreePartition(
        long nodeCount,
        long relationshipCount,
        int concurrency,
        DegreeFunction degrees
    ) {
        long numRelationshipsInPartition = Math.floorDiv(relationshipCount, concurrency * DIVISION_FACTOR);

        Stream.Builder<DegreePartition> streamBuilder = Stream.builder();

        if (nodeCount <= 0) {
            return streamBuilder.build();
        }

        long currentStartNode = 0;
        long currentRelationshipCount = degrees.degree(0);
        for (long i = 1; i < nodeCount; ++i) {
            long nextRelationshipCount = degrees.degree(i);
            if (currentRelationshipCount + nextRelationshipCount > numRelationshipsInPartition) {
                streamBuilder.add(DegreePartition.of(currentStartNode, i - currentStartNode, currentRelationshipCount));
            } else {

            }
        }

        return streamBuilder.build();
    }

    public static <TASK> List<TASK> degreePartitionWithBatchSize(
        Graph graph,
        long batchSize,
        Function<DegreePartition, TASK> taskCreator
    ) {
        return degreePartitionWithBatchSize(graph.nodeCount(), graph::degree, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> degreePartitionWithBatchSize(
        long nodeCount,
        DegreeFunction degrees,
        long batchSize,
        Function<DegreePartition, TASK> taskCreator
    ) {
        var partitions = new ArrayList<DegreePartition>();
        long start = 0L;

        assert batchSize > 0L;

        long minPartitionSize = Math.round(batchSize * MIN_PARTITION_CAPACITY);

        while(start < nodeCount) {
            long partitionSize = 0L;

            long nodeId = start - 1;
            // find the next partition
            while (nodeId < nodeCount - 1 && nodeId - start < Partition.MAX_NODE_COUNT) {
                int degree = degrees.degree(nodeId + 1);

                boolean partitionIsLargeEnough = partitionSize >= minPartitionSize;
                if (partitionSize + degree > batchSize && partitionIsLargeEnough) {
                    break;
                }

                nodeId++;
                partitionSize += degree;
            }

            long end = nodeId + 1;
            partitions.add(DegreePartition.of(start, end - start, partitionSize));
            start = end;
        }

        // the above loop only merge partition i with i+1 to avoid i being too small
        // thus we need to check the last partition manually
        var minLastPartitionSize = Math.round(0.2 * batchSize);
        if (partitions.size() > 1 && partitions.get(partitions.size() - 1).totalDegree() < minLastPartitionSize) {
            var lastPartition = partitions.remove(partitions.size() - 1);
            var partitionToMerge = partitions.remove(partitions.size() - 1);

            DegreePartition mergedPartition = DegreePartition.of(
                partitionToMerge.startNode(),
                lastPartition.nodeCount() + partitionToMerge.nodeCount(),
                partitionToMerge.totalDegree() + lastPartition.totalDegree()
            );

            partitions.add(mergedPartition);
        }

        return partitions.stream().map(taskCreator).collect(Collectors.toList());
    }

    public static <TASK> List<TASK> degreePartitionWithBatchSize(
        BitSet bitset,
        DegreeFunction degrees,
        long degreesPerBatch,
        Function<IteratorPartition, TASK> taskCreator
    ) {
        assert degreesPerBatch > 0L;

        var iterator = bitset.iterator();
        var totalSize = bitset.cardinality();

        var result = new ArrayList<TASK>();
        long seen = 0L;

        while (seen < totalSize) {
            long setBit = iterator.nextSetBit();
            long currentDegrees = degrees.degree(setBit);
            long currentLength = 1L;
            long startIdx = setBit;
            seen++;

            while (seen < totalSize && currentDegrees < degreesPerBatch && currentLength < Partition.MAX_NODE_COUNT) {
                setBit = iterator.nextSetBit();
                currentDegrees += degrees.degree(setBit);
                currentLength++;
                seen++;
            }

            result.add(taskCreator.apply(new IteratorPartition(new SetBitsIterable(bitset, startIdx).iterator(), currentLength)));
        }

        return result;
    }

    @FunctionalInterface
    public interface DegreeFunction {
        int degree(long node);
    }

    private static <TASK> List<TASK> tasks(
        long nodeCount,
        long batchSize,
        Function<Partition, TASK> taskCreator
    ) {
        var expectedCapacity = Math.toIntExact(BitUtil.ceilDiv(nodeCount, batchSize));
        var result = new ArrayList<TASK>(expectedCapacity);
        for (long i = 0; i < nodeCount; i += batchSize) {
            result.add(taskCreator.apply(Partition.of(i, actualBatchSize(i, batchSize, nodeCount))));
        }
        return result;
    }

    private static long actualBatchSize(long startNode, long batchSize, long nodeCount) {
        return startNode + batchSize < nodeCount ? batchSize : nodeCount - startNode;
    }

    public static List<Long> rangePartitionActualBatchSizes(
        int concurrency,
        long nodeCount,
        Optional<Integer> minBatchSize
    ) {
        long batchSize = ParallelUtil.adjustedBatchSize(
            nodeCount,
            concurrency,
            minBatchSize.orElse(ParallelUtil.DEFAULT_BATCH_SIZE)
        );
        var expectedCapacity = Math.toIntExact(BitUtil.ceilDiv(nodeCount, batchSize));
        var batchSizes = new ArrayList<Long>(expectedCapacity);

        for (long i = 0; i < nodeCount; i += batchSize) {
            batchSizes.add(actualBatchSize(i, batchSize, nodeCount));
        }

        return batchSizes;
    }

    public static <TASK> Iterator<TASK> blockAlignedPartitioning(
        HugeLongArray sortedIds,
        int blockShift,
        Function<Partition, TASK> taskCreator
    ) {
        return new BlockAlignedPartitionIterator<>(sortedIds, blockShift, taskCreator);
    }

    private static class BlockAlignedPartitionIterator<TASK> extends AbstractIterator<TASK> {
        private final HugeCursor<long[]> cursor;
        private final long size;
        private final int blockShift;
        private final Function<Partition, TASK> taskCreator;

        private int prevBlockId;
        private long blockStart;
        private boolean done;
        private int lastIndex;

        BlockAlignedPartitionIterator(
            HugeLongArray sortedIds,
            int blockShift,
            Function<Partition, TASK> taskCreator
        ) {
            this.size = sortedIds.size();
            this.blockShift = blockShift;
            this.taskCreator = taskCreator;
            this.cursor = sortedIds.initCursor(sortedIds.newCursor());
            this.prevBlockId = 0;
            this.blockStart = 0L;
            this.done = false;
            this.lastIndex = Integer.MAX_VALUE;
        }

        @Override
        protected TASK fetch() {
            if (this.done) {
                return done();
            }

            long base = cursor.base;
            int limit = cursor.limit;
            long[] array = cursor.array;
            int prevBlockId = this.prevBlockId;
            int blockShift = this.blockShift;

            for (int i = lastIndex; i < limit; i++) {
                long originalId = array[i];
                int blockId = (int) (originalId >>> blockShift);
                if (blockId != prevBlockId) {
                    long internalId = base + i;
                    prevBlockId = blockId;

                    if (internalId > 0) {
                        var partition = Partition.of(blockStart, internalId - blockStart);
                        this.blockStart = internalId;
                        this.prevBlockId = prevBlockId;
                        this.lastIndex = i;
                        return taskCreator.apply(partition);
                    }
                }
            }

            if (cursor.next()) {
                this.prevBlockId = prevBlockId;
                this.lastIndex = cursor.offset;
                return fetch();
            }

            var partition = Partition.of(blockStart, size - blockStart);
            this.done = true;

            return taskCreator.apply(partition);
        }
    }
}
