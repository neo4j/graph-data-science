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
package org.neo4j.gds.leiden;

import org.neo4j.gds.ImmutableRelationshipProjections;
import org.neo4j.gds.NodeProjections;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipProjection;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.ImmutableGraphDimensions;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.NativeFactory;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

class GraphAggregationPhase {

    static MemoryEstimation memoryEstimation() {
        return MemoryEstimations.builder(GraphAggregationPhase.class)
            .rangePerGraphDimension("aggregated graph", (rootGraphDimensions, concurrency) -> {
                // The input graph might have multiple node and relationship properties
                // but the aggregated graph will never have more than a single relationship property
                // so let's
                var maxDimensions = ImmutableGraphDimensions
                    .builder()
                    .from(rootGraphDimensions)
                    .build();

                var minDimensions = ImmutableGraphDimensions
                    .builder()
                    .nodeCount(2)
                    .highestPossibleNodeCount(2)
                    .relationshipCounts(Map.of(RelationshipType.of("foo"), 1L))
                    .relCountUpperBound(1)
                    .highestRelationshipId(1)
                    .build();

                var relationshipProjections = ImmutableRelationshipProjections.builder()
                    .putProjection(
                        RelationshipType.of("AGGREGATE"),
                        RelationshipProjection.builder()
                            .type("AGGREGATE")
                            .orientation(Orientation.UNDIRECTED)
                            .aggregation(Aggregation.SUM)
                            .addProperty("prop", "prop", DefaultValue.of(1.0))
                            .build()
                    )
                    .build();

                var memoryEstimation = NativeFactory.getMemoryEstimation(
                    NodeProjections.all(),
                    relationshipProjections,
                    false
                );
                var min = memoryEstimation.estimate(minDimensions, concurrency).memoryUsage().min;
                var max = memoryEstimation.estimate(maxDimensions, concurrency).memoryUsage().max;

                return MemoryRange.of(min, max);
            }).perNode("sorted communities", HugeLongArray::memoryEstimation)
            .perNode("atomic coordination array", HugeAtomicLongArray::memoryEstimation).
            build();
    }

    private final Graph workingGraph;
    private final HugeLongArray communities;
    private final Direction direction;
    private final long maxCommunityId;
    private final ExecutorService executorService;
    private final int concurrency;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;

    GraphAggregationPhase(
        Graph workingGraph,
        Direction direction,
        HugeLongArray communities,
        long maxCommunityId,
        ExecutorService executorService,
        int concurrency,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.workingGraph = workingGraph;
        this.communities = communities;
        this.direction = direction;
        this.maxCommunityId = maxCommunityId;
        this.executorService = executorService;
        this.concurrency = concurrency;

        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
    }

    Graph run() {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(maxCommunityId)
            .concurrency(this.concurrency)
            .build();

        terminationFlag.assertRunning();

        ParallelUtil.parallelForEachNode(
            workingGraph.nodeCount(),
            concurrency,
            (nodeId) -> {
                nodesBuilder.addNode(communities.get(nodeId));
            }
        );

        terminationFlag.assertRunning();

        IdMap idMap = nodesBuilder.build().idMap();
        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .orientation(direction.toOrientation())
            .addPropertyConfig(Aggregation.SUM, DefaultValue.forDouble())
            .executorService(executorService)
            .build();

        var sortedNodesByCommunity = getNodesSortedByCommunity(
            communities,
            concurrency
        );

        Function<Long, Integer> customDegree = x -> workingGraph.degree(sortedNodesByCommunity.get(x));
        var relationshipCreators = PartitionUtils.customDegreePartitionWithBatchSize(
            workingGraph,
            concurrency,
            customDegree,
            partition ->
                new RelationshipCreator(
                    sortedNodesByCommunity,
                    communities,
                    partition,
                    relationshipsBuilder,
                    workingGraph.concurrentCopy(),
                    direction,
                    progressTracker
                ),
            Optional.empty(),
            Optional.of(workingGraph.relationshipCount())
        );

        ParallelUtil.run(relationshipCreators, executorService);

        return GraphFactory.create(idMap, relationshipsBuilder.build());
    }

    static HugeLongArray getNodesSortedByCommunity(HugeLongArray communities, int concurrency) {
        long nodeCount = communities.size();

        var sortedNodesByCommunity = HugeLongArray.newArray(nodeCount);
        HugeAtomicLongArray communityCoordinateArray = HugeAtomicLongArray.newArray(nodeCount);


        ParallelUtil.parallelForEachNode(nodeCount, concurrency, nodeId -> {
            {
                long communityId = communities.get(nodeId);
                communityCoordinateArray.getAndAdd(communityId, 1);
            }
        });
        AtomicLong atomicNodeSum = new AtomicLong();
        ParallelUtil.parallelForEachNode(nodeCount, concurrency, indexId ->
        {
            if (communityCoordinateArray.get(indexId) > 0) {
                var nodeSum = atomicNodeSum.addAndGet(communityCoordinateArray.get(indexId));
                communityCoordinateArray.set(indexId, nodeSum);
            }
        });

        ParallelUtil.parallelForEachNode(nodeCount, concurrency, indexId ->
        {

            long nodeId = nodeCount - indexId - 1;
            long communityId = communities.get(nodeId);
            long coordinate = communityCoordinateArray.getAndAdd(communityId, -1);
            sortedNodesByCommunity.set(coordinate - 1, nodeId);

        });

        return sortedNodesByCommunity;

    }

}
