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
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphdb.Direction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.neo4j.graphalgo.core.utils.BitUtil.ceilDiv;
import static org.neo4j.graphalgo.core.utils.partition.Partition.MAX_NODE_COUNT;

public abstract class DegreePartitioning implements Partitioning {

    final int concurrency;
    final long batchSize;
    final Direction direction;
    final Graph graph;
    final Optional<BitSet> maybeNodeFilter;

    public DegreePartitioning(
        int concurrency,
        long batchSize,
        Direction direction,
        Graph graph,
        Optional<BitSet> maybeNodeFilter
    ) {
        this.concurrency = concurrency;
        this.batchSize = batchSize;
        this.direction = direction;
        this.graph = graph;
        this.maybeNodeFilter = maybeNodeFilter;
    }

    protected List<Partition> partitionFromConcurrency() {
        BitSet nodeFilter;
        long nodeCount = graph.nodeCount();

        if (maybeNodeFilter.isPresent()) {
            nodeFilter = maybeNodeFilter.get();
        } else {
            BitSet unitBitSet = new BitSet(nodeCount);
            unitBitSet.set(0, nodeCount);
            nodeFilter = unitBitSet;
        }

        long cumulativeDegree = graph.relationshipCount();
        long adjustedDegree = ceilDiv(cumulativeDegree, nodeCount) * nodeFilter.cardinality();

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
        while (currentNode >= 0 && currentNode < nodeCount - 1) {
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

    protected List<Partition> partitionFromBatchSize() {
        PrimitiveLongIterator nodes = graph.nodeIterator();
        List<Partition> partitions = new ArrayList<>();
        long start = 0L;
        while (nodes.hasNext()) {

            assert batchSize > 0L;
            int nodeCount = 0;
            long partitionSize = 0L;
            while (nodes.hasNext() && partitionSize < batchSize && nodeCount < MAX_NODE_COUNT) {
                long nodeId = nodes.next();
                ++nodeCount;
                partitionSize += graph.degree(nodeId, direction);
            }

            partitions.add(new Partition(start, nodeCount));
            start += nodeCount;
        }
        return partitions;
    }

    public static List<Partition> fromConcurrency(
        int concurrency,
        long batchSize,
        Direction direction,
        Graph graph,
        Optional<BitSet> maybeNodeFilter
    ) {
        return new DegreePartitioning(
            concurrency,
            batchSize,
            direction,
            graph,
            maybeNodeFilter
        ) {
            @Override
            public List<Partition> compute() {
                return partitionFromConcurrency();
            }
        }.compute();
    }

    public static List<Partition> fromBatchSize(
        long batchSize,
        Direction direction,
        Graph graph
    ) {
        return new DegreePartitioning(
            -1,
            batchSize,
            direction,
            graph,
            Optional.empty()
        ) {
            @Override
            public List<Partition> compute() {
                return partitionFromBatchSize();
            }
        }.compute();
    }
}
