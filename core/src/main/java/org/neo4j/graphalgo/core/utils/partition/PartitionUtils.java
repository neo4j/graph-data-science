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

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.neo4j.graphalgo.core.utils.partition.Partition.MAX_NODE_COUNT;

public final class PartitionUtils {

    private PartitionUtils() {}

    public static <TASK> List<TASK> rangePartition(int concurrency, long nodeCount, Function<Partition, TASK> taskCreator) {
        long batchSize = ParallelUtil.adjustedBatchSize(nodeCount, concurrency, ParallelUtil.DEFAULT_BATCH_SIZE);
        return rangePartition(concurrency, nodeCount, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> rangePartition(int concurrency, long nodeCount, long batchSize, Function<Partition, TASK> taskCreator) {
        var result = new ArrayList<TASK>(concurrency);
        for (long i = 0; i < nodeCount; i += batchSize) {
            long actualBatchSize = i + batchSize < nodeCount ? batchSize : nodeCount - i;
            result.add(taskCreator.apply(Partition.of(i, actualBatchSize)));
        }

        return result;
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

    public static <TASK> List<TASK> degreePartition(Graph graph, long batchSize, Function<Partition, TASK> taskCreator) {
        return degreePartition(graph.nodeIterator(), graph, batchSize, taskCreator);
    }

    public static <TASK> List<TASK> degreePartition(
        PrimitiveLongIterator nodes,
        Degrees degrees,
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

}
