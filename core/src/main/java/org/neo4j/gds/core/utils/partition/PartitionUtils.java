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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeCursor;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.mem.BitUtil;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.function.LongToIntFunction;
import java.util.stream.LongStream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class PartitionUtils {

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
        var batchSize = Math.max(
            minBatchSize.orElse(ParallelUtil.DEFAULT_BATCH_SIZE),
            BitUtil.ceilDiv(graph.relationshipCount(), concurrency)
        );
        return degreePartitionWithBatchSize(graph.nodeIterator(), graph::degree, batchSize, taskCreator);
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
        return degreePartitionWithBatchSize(graph.nodeIterator(), customDegreeFunction::applyAsInt, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> degreePartitionWithBatchSize(
        Graph graph,
        long batchSize,
        Function<DegreePartition, TASK> taskCreator
    ) {
        return degreePartitionWithBatchSize(graph.nodeIterator(), graph::degree, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> degreePartitionWithBatchSize(
        PrimitiveIterator.OfLong nodes,
        DegreeFunction degrees,
        long batchSize,
        Function<DegreePartition, TASK> taskCreator
    ) {
        var result = new ArrayList<TASK>();
        long start = 0L;
        while (nodes.hasNext()) {
            assert batchSize > 0L;
            long partitionSize = 0L;
            long nodeId = 0L;
            while (nodes.hasNext() && partitionSize <= batchSize && nodeId - start < Partition.MAX_NODE_COUNT) {
                nodeId = nodes.nextLong();
                partitionSize += degrees.degree(nodeId);
            }

            long end = nodeId + 1;
            result.add(taskCreator.apply(DegreePartition.of(start, end - start, partitionSize)));
            start = end;
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
