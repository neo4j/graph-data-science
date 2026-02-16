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
package org.neo4j.gds.paths.delta;

import com.carrotsearch.hppc.DoubleArrayList;
import com.carrotsearch.hppc.LongArrayList;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.collections.haa.HugeAtomicDoubleArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.neo4j.gds.paths.delta.TentativeDistances.NO_PREDECESSOR;

public record DeltaSteppingResult(TentativeDistances tentativeDistances,PathFindingResult pathFindingResult) {

    public static DeltaSteppingResult empty() {
        return new DeltaSteppingResult(
            TentativeDistances.distanceAndPredecessors(0,new Concurrency(1)),
            PathFindingResult.empty()
        );
    }
    static DeltaSteppingResult create(
        TentativeDistances tentativeDistances,
        Concurrency concurrency,
        long sourceNode
    ){
        return new DeltaSteppingResult(
            tentativeDistances,
            new PathFindingResult(pathResults(tentativeDistances,sourceNode,concurrency))
        );
    }

    private static Stream<PathResult> pathResults(
        TentativeDistances tentativeDistances,
        long sourceNode,
        Concurrency concurrency
    ) {
        var distances = tentativeDistances.distances();
        var predecessors = tentativeDistances.predecessors().orElseThrow();

        var pathIndex = new AtomicLong(0L);

        var partitions = PartitionUtils.rangePartition(
            concurrency,
            predecessors.size(),
            partition -> partition,
            Optional.empty()
        );

        return ParallelUtil.parallelStream(
            partitions.stream(),
            concurrency,
            parallelStream -> parallelStream.flatMap(partition -> {
                var localPathIndex = new MutableLong(pathIndex.getAndAdd(partition.nodeCount()));

                var pathNodeIds = new LongArrayList();
                var costs = new DoubleArrayList();

                return LongStream
                    .range(partition.startNode(), partition.startNode() + partition.nodeCount())
                    .filter(target -> predecessors.get(target) != NO_PREDECESSOR)
                    .mapToObj(targetNode -> pathResult(
                        localPathIndex.getAndIncrement(),
                        sourceNode,
                        targetNode,
                        distances,
                        predecessors,
                        pathNodeIds,
                        costs
                    ));
            })
        );
    }

    private static PathResult pathResult(
        long pathIndex,
        long sourceNode,
        long targetNode,
        HugeAtomicDoubleArray distances,
        HugeAtomicLongArray predecessors,
        LongArrayList pathNodeIds,
        DoubleArrayList costs
    ) {
        // We backtrack until we reach the source node.
        var lastNode = targetNode;

        while (true) {
            pathNodeIds.add(lastNode);
            costs.add(distances.get(lastNode));

            // Break if we reach the end by hitting the source node.
            if (lastNode == sourceNode) {
                break;
            }

            lastNode = predecessors.get(lastNode);
        }

        var pathNodeIdsArray = pathNodeIds.toArray();
        ArrayUtils.reverse(pathNodeIdsArray);
        pathNodeIds.elementsCount = 0;
        var costsArray = costs.toArray();
        ArrayUtils.reverse(costsArray);
        costs.elementsCount = 0;
        return new DeltaSteppingPathResult(pathIndex, sourceNode, targetNode, pathNodeIdsArray, costsArray);
    }

}
