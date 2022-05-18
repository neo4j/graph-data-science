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

import com.carrotsearch.hppc.LongLongHashMap;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

//TODO: take care of potential issues w. self-loops

public class Leiden extends Algorithm<LeidenResult> {

    private final Graph rootGraph;

    private final int maxIterations;
    private final double gamma;
    private final double theta;

    private final HugeLongArray[] dendrograms;

    private final ExecutorService executorService;
    private final int concurrency;
    private final long seed;

    public Leiden(
        Graph graph,
        int maxIterations,
        double gamma,
        double theta,
        long seed,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.rootGraph = graph;
        this.maxIterations = maxIterations;
        this.gamma = gamma;
        this.theta = theta;
        this.seed = seed;

        // TODO: Pass these two as parameters
        this.executorService = Pools.DEFAULT;
        this.concurrency = concurrency;

        this.dendrograms = new HugeLongArray[maxIterations];
    }

    @Override
    public LeidenResult compute() {
        var workingGraph = rootGraph;
        var orientation = rootGraph.isUndirected() ? Orientation.UNDIRECTED : Orientation.NATURAL;

        var nodeVolumes = HugeDoubleArray.newArray(workingGraph.nodeCount());
        var communityVolumes = HugeDoubleArray.newArray(workingGraph.nodeCount());
        initVolumes(nodeVolumes, communityVolumes);

        HugeLongArray partition = HugeLongArray.newArray(workingGraph.nodeCount());
        partition.setAll(nodeId -> nodeId);

        boolean didConverge = false;
        int iteration;
        // move on with refinement -> aggregation -> local move again
        for (iteration = 0; iteration < maxIterations; iteration++) {

            // 1. LOCAL MOVE PHASE - over the singleton partition
            var localMovePhase = LocalMovePhase.create(workingGraph, partition, nodeVolumes, communityVolumes, gamma);
            var localMovePhasePartition = localMovePhase.run();

            partition = localMovePhasePartition.communities();
            communityVolumes = localMovePhasePartition.communityVolumes();
            var communitiesCount = Arrays.stream(partition.toArray()).distinct().count();

            didConverge = communitiesCount == workingGraph.nodeCount();
            if (didConverge) {
                break;
            }

            // 2 REFINE
            var refinementPhase = RefinementPhase.create(
                workingGraph,
                partition,
                nodeVolumes,
                communityVolumes,
                gamma,
                theta,
                seed
            );
            var refinedPhasePartition = refinementPhase.run();
            var refinedPartition = refinedPhasePartition.communities();

            var refinedCommunityVolumes = refinedPhasePartition.communityVolumes();
            long maxCommunityId = buildDendrogram(workingGraph, iteration, refinedPartition);

            // 3 CREATE NEW GRAPH
            var graphAggregationPhase = new GraphAggregationPhase(
                workingGraph,
                orientation,
                refinedPartition,
                maxCommunityId,
                this.executorService,
                this.concurrency,
                this.terminationFlag
            );
            workingGraph = graphAggregationPhase.run();

            // Post-aggregate step: MAINTAIN PARTITION
            var communityData = maintainPartition(
                workingGraph,
                partition,
                refinedCommunityVolumes
            );
            partition = communityData.seededCommunitiesForNextIteration;
            communityVolumes = communityData.communityVolumes;
            nodeVolumes = communityData.aggregatedNodeSeedVolume;
        }

        return LeidenResult.of(dendrograms[iteration - 1], iteration, didConverge);
    }

    private void initVolumes(HugeDoubleArray nodeVolumes, HugeDoubleArray communityVolumes) {
        if (rootGraph.hasRelationshipProperty()) {
            ParallelUtil.parallelForEachNode(
                rootGraph.nodeCount(),
                concurrency,
                nodeId -> {
                    rootGraph.concurrentCopy().forEachRelationship(nodeId, 1.0, (s, t, w) -> {
                        nodeVolumes.addTo(nodeId, w);
                        communityVolumes.addTo(nodeId, w);
                        return true;
                    });
                }
            );
        } else {
            nodeVolumes.setAll(rootGraph::degree);
            communityVolumes.setAll(rootGraph::degree);
        }
    }

    private long buildDendrogram(
        Graph workingGraph,
        int iteration,
        HugeLongArray currentCommunities
    ) {
        dendrograms[iteration] = HugeLongArray.newArray(rootGraph.nodeCount());
        AtomicLong maxCommunityId = new AtomicLong(0L);
        ParallelUtil.parallelForEachNode(rootGraph, concurrency, (nodeId) -> {
            long prevId = iteration == 0
                ? nodeId
                : workingGraph.toMappedNodeId(dendrograms[iteration - 1].get(nodeId));

            long communityId = currentCommunities.get(prevId);

            boolean updatedMaxCurrentId;
            do {
                var currentMaxId = maxCommunityId.get();
                if (communityId > currentMaxId) {
                    updatedMaxCurrentId = maxCommunityId.compareAndSet(currentMaxId, communityId);
                } else {
                    updatedMaxCurrentId = true;
                }
            } while (!updatedMaxCurrentId);

            dendrograms[iteration].set(nodeId, communityId);
        });

        return maxCommunityId.get();
    }

    static @NotNull CommunityData maintainPartition(
        Graph workingGraph,
        @NotNull HugeLongArray localPhaseCommunities,
        HugeDoubleArray refinedCommunityVolumes
    ) {

        HugeLongArray seededCommunitiesForNextIteration = HugeLongArray.newArray(workingGraph.nodeCount());

        var localPhaseCommunityToAggregatedNewId = new LongLongHashMap();

        //this works under the following constraint:
        //   for every  mapping community x
        //  nodeId  x from the previous graph (i.e., originalNode) is in same  community x
        //Otherwise, we need a reverse Map (see Louvain)

        //refined : corresponds to the refined communities in the previous step (in their original ids)
        //aggregated: corresponds to the refined communities in the previous step (in id in the new graph)
        //localPhase: corresponds to the local phase communities in the previous step
        HugeDoubleArray aggregatedCommunitySeedVolume = HugeDoubleArray.newArray(workingGraph.nodeCount());
        HugeDoubleArray aggregatedNodeSeedVolume = HugeDoubleArray.newArray(workingGraph.nodeCount());
        workingGraph.forEachNode(aggregatedCommunityId -> {
            var refinedCommunityId = workingGraph.toOriginalNodeId(aggregatedCommunityId);
            long localPhaseCommunity = localPhaseCommunities.get(refinedCommunityId);
            long aggregatedSeedCommunity;

            double volumeOfTheAggregatedCommunity = refinedCommunityVolumes.get(refinedCommunityId);

            if (localPhaseCommunityToAggregatedNewId.containsKey(localPhaseCommunity)) {
                aggregatedSeedCommunity = localPhaseCommunityToAggregatedNewId.get(localPhaseCommunity);
            } else {
                aggregatedSeedCommunity = aggregatedCommunityId;
                localPhaseCommunityToAggregatedNewId.put(localPhaseCommunity, aggregatedSeedCommunity);
            }
            aggregatedCommunitySeedVolume.addTo(aggregatedSeedCommunity, volumeOfTheAggregatedCommunity);

            seededCommunitiesForNextIteration.set(aggregatedCommunityId, aggregatedSeedCommunity);
            aggregatedNodeSeedVolume.set(aggregatedCommunityId, volumeOfTheAggregatedCommunity);
            return true;
        });
        return new CommunityData(seededCommunitiesForNextIteration, aggregatedCommunitySeedVolume, aggregatedNodeSeedVolume);
    }

    @Override
    public void release() {

    }

}
