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
package org.neo4j.graphalgo.core.utils.partition;

import com.carrotsearch.hppc.AbstractIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.neo4j.graphalgo.core.utils.partition.Partition.MAX_NODE_COUNT;

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
        final long initialBatchSize = ParallelUtil.adjustedBatchSize(nodeCount, concurrency, alignTo);
        final long remainder = initialBatchSize % alignTo;
        final long adjustedBatchSize = remainder == 0 ? initialBatchSize : initialBatchSize + (alignTo - remainder);
        List<Partition> partitions = new ArrayList<>(concurrency);
        for (long i = 0; i < nodeCount; i += adjustedBatchSize) {
            long actualBatchSize = i + adjustedBatchSize < nodeCount ? adjustedBatchSize : nodeCount - i;
            partitions.add(Partition.of(i, actualBatchSize));
        }

        return partitions;
    }

    public static <TASK> List<TASK> numberAlignedPartitioning(
        int concurrency,
        long nodeCount,
        long alignTo,
        Function<Partition, TASK> taskCreator
    ) {
        final long initialBatchSize = ParallelUtil.adjustedBatchSize(nodeCount, concurrency, alignTo);
        final long remainder = initialBatchSize % alignTo;
        final long adjustedBatchSize = remainder == 0 ? initialBatchSize : initialBatchSize + (alignTo - remainder);
        return tasks(nodeCount, adjustedBatchSize, taskCreator);
    }

    public static <TASK> List<TASK> degreePartition(
        Graph graph,
        int concurrency,
        Function<Partition, TASK> taskCreator,
        Optional<Integer> minBatchSize
    ) {
        var batchSize = Math.max(
            minBatchSize.orElse(ParallelUtil.DEFAULT_BATCH_SIZE),
            BitUtil.ceilDiv(graph.relationshipCount(), concurrency)
        );
        return degreePartitionWithBatchSize(graph.nodeIterator(), graph::degree, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> degreePartitionWithBatchSize(Graph graph, long batchSize, Function<Partition, TASK> taskCreator) {
        return degreePartitionWithBatchSize(graph.nodeIterator(), graph::degree, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> degreePartitionWithBatchSize(
        PrimitiveLongIterator nodes,
        DegreeFunction degrees,
        long batchSize,
        Function<Partition, TASK> taskCreator
    ) {
        var result = new ArrayList<TASK>();
        long start = 0L;
        while (nodes.hasNext()) {
            assert batchSize > 0L;
            long partitionSize = 0L;
            long nodeId = 0L;
            while (nodes.hasNext() && partitionSize <= batchSize && nodeId - start < MAX_NODE_COUNT) {
                nodeId = nodes.next();
                partitionSize += degrees.degree(nodeId);
            }

            long end = nodeId + 1;
            result.add(taskCreator.apply(Partition.of(start, end - start)));
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
            long actualBatchSize = i + batchSize < nodeCount ? batchSize : nodeCount - i;
            result.add(taskCreator.apply(Partition.of(i, actualBatchSize)));
        }
        return result;
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
                        var partition = ImmutablePartition.of(blockStart, internalId - blockStart);
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

            var partition = ImmutablePartition.of(blockStart, size - blockStart);
            this.done = true;

            return taskCreator.apply(partition);
        }
    }
}
