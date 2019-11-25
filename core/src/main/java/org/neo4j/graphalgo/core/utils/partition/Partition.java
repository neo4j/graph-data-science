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

package org.neo4j.graphalgo.core.utils.partition;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;

public class Partition {

    public static final int MAX_NODE_COUNT = (Integer.MAX_VALUE - 32) >> 1;

    public final long startNode;
    public final long nodeCount;

    public Partition(long startNode, long nodeCount) {
        this.startNode = startNode;
        this.nodeCount = nodeCount;
    }

    public boolean fits(int otherPartitionsCount) {
        return MAX_NODE_COUNT - otherPartitionsCount >= nodeCount;
    }

    public static List<Partition> nodePartitioning(
        long graphNodeCount,
        int concurrency,
        long batchSize
    ) {
        long partitionSize = ParallelUtil.adjustedBatchSize(graphNodeCount, concurrency, batchSize);
        List<Partition> partitions = new ArrayList<>((int) ceilDiv(graphNodeCount, partitionSize));
        for (long i = 0; i < graphNodeCount; i += partitionSize) {
            long nodeCountInPartition = partitionSize;
            if (i + partitionSize > graphNodeCount) {
                partitionSize = graphNodeCount - i;
            }
            partitions.add(
                new Partition(i, nodeCountInPartition)
            );
        }

        return partitions;
    }

    public static List<Partition> degreePartitioning(
        long graphNodeCount,
        int concurrency,
        long batchSize,
        Direction direction,
        Graph graph,
        Optional<BitSet> maybeNodeFilter
    ) {
        BitSet nodeFilter;
        if (maybeNodeFilter.isPresent()) {
            nodeFilter = maybeNodeFilter.get();
        } else {
            BitSet unitBitSet = new BitSet(graphNodeCount);
            unitBitSet.set(0, graphNodeCount);
            nodeFilter = unitBitSet;
        }

        long cumulativeDegree = graph.relationshipCount();
        long adjustedDegree = ceilDiv(cumulativeDegree, graphNodeCount) * nodeFilter.cardinality();

        long batchDegree = ParallelUtil.adjustedBatchSize(
            adjustedDegree,
            concurrency,
            batchSize,
            Integer.MAX_VALUE
        );

        if (direction == Direction.BOTH) {
            batchDegree *= 2;
        }

        List<Partition> partitions = new ArrayList<>(concurrency);
        long currentNode = nodeFilter.nextSetBit(0);
        long lastNode = 0;
        long batchStart = currentNode;
        long currentDegree = 0L;
        while (currentNode >= 0 && currentNode < graphNodeCount - 1) {
            currentDegree += graph.degree(currentNode, direction);

            if (currentDegree >= batchDegree) {
                partitions.add(new Partition(batchStart, currentNode - batchStart));
                currentNode++;
                batchStart = currentNode;
                currentDegree = 0;
            }
            lastNode = currentNode;
            currentNode = nodeFilter.nextSetBit(currentNode + 1);
        }

        if (currentDegree > 0) {
            partitions.add(new Partition(batchStart, Math.min(lastNode + 1, graph.nodeCount())));
        }

        return partitions;
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
        for (long i = 0; i < nodeCount; i+=adjustedBatchSize) {
            partitions.add(new Partition(i, Math.min(nodeCount, i + adjustedBatchSize - 1)));
        }

        return partitions;
    }
}
