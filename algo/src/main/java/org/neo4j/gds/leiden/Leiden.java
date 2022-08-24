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
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

//TODO: take care of potential issues w. self-loops

public class Leiden extends Algorithm<LeidenResult> {

    private final Graph rootGraph;
    private final Orientation orientation;

    private final int maxIterations;
    private final double initialGamma;
    private final double theta;

    private double[] modularities;

    private double modularity;
    private final LeidenDendrogramManager dendrogramManager;

    private final ExecutorService executorService;
    private final int concurrency;
    private final long seed;


    public Leiden(
        Graph graph,
        int maxIterations,
        double initialGamma,
        double theta,
        boolean includeIntermediateCommunities,
        long seed,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.rootGraph = graph;
        this.orientation = rootGraph.schema().isUndirected() ? Orientation.UNDIRECTED : Orientation.NATURAL;
        this.maxIterations = maxIterations;
        this.initialGamma = initialGamma;
        this.theta = theta;
        this.seed = seed;
        // TODO: Pass these two as parameters
        this.executorService = Pools.DEFAULT;
        this.concurrency = concurrency;
        this.dendrogramManager = new LeidenDendrogramManager(
            graph.nodeCount(),
            maxIterations,
            includeIntermediateCommunities
        );

        this.modularities = new double[maxIterations];
        this.modularity = 0d;
    }

    @Override
    public LeidenResult compute() {
        var workingGraph = rootGraph;
        var nodeCount = workingGraph.nodeCount();

        var seedCommunities = HugeLongArray.newArray(nodeCount);
        seedCommunities.setAll(v -> v);

        // volume -> the sum of the weights of a nodes outgoing relationships
        var nodeVolumes = HugeDoubleArray.newArray(nodeCount);
        // the sum of the node volume for all nodes in a community
        var communityVolumes = HugeDoubleArray.newArray(nodeCount);
        double modularityScaleCoefficient = initVolumes(nodeVolumes, communityVolumes, seedCommunities);

        double gamma = this.initialGamma * modularityScaleCoefficient;

        var communityCount = nodeCount;
        var partition = HugeLongArray.newArray(communityCount);
        partition.setAll(v -> v);

        HugeLongArray currentDendrogram = null;

        boolean didConverge = false;
        int iteration;
        for (iteration = 0; iteration < maxIterations; iteration++) {
            // 1. LOCAL MOVE PHASE - over the singleton partition
            var localMovePhase = LocalMovePhase.create(
                workingGraph,
                partition,
                nodeVolumes,
                communityVolumes,
                gamma,
                communityCount
            );
            var communitiesCount = localMovePhase.run();

            didConverge = communitiesCount == workingGraph.nodeCount() || localMovePhase.swaps == 0;
            if (didConverge) {
                break;
            }

            modularities[iteration] = ModularityComputer.modularity(
                workingGraph,
                partition,
                communityVolumes,
                gamma,
                modularityScaleCoefficient,
                concurrency,
                executorService
            );

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
            var refinementPhaseResult = refinementPhase.run();
            var refinedPartition = refinementPhaseResult.communities();
            var refinedCommunityVolumes = refinementPhaseResult.communityVolumes();

            var dendrogramResult = buildDendrogram(workingGraph, currentDendrogram, refinedPartition);
            currentDendrogram = dendrogramResult.dendrogram();

            dendrogramManager.prepareNextLevel(iteration);
            for (long v = 0; v < rootGraph.nodeCount(); v++) {
                var currentRefinedCommunityId = currentDendrogram.get(v);
                var actualCommunityId = partition.get(currentRefinedCommunityId);
                dendrogramManager.set(v, actualCommunityId);
            }

            // 3 CREATE NEW GRAPH
            var graphAggregationPhase = new GraphAggregationPhase(
                workingGraph,
                this.orientation,
                refinedPartition,
                dendrogramResult.maxCommunityId(),
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
            communityCount = communityData.communityCount;

            seedCommunities = dendrogramManager.getCurrent();
            modularity = modularities[iteration];
        }
        return LeidenResult.of(
            seedCommunities,
            iteration,
            didConverge,
            dendrogramManager,
            resizeModularitiesArray(iteration),
            modularity
        );
    }

    private double initVolumes(HugeDoubleArray nodeVolumes, HugeDoubleArray communityVolumes, HugeLongArray seedCommunities) {
        if (rootGraph.hasRelationshipProperty()) {
            ParallelUtil.parallelForEachNode(
                rootGraph.nodeCount(),
                concurrency,
                nodeId -> {
                    rootGraph.concurrentCopy().forEachRelationship(nodeId, 1.0, (s, t, w) -> {
                        nodeVolumes.addTo(nodeId, w);
                        communityVolumes.addTo(seedCommunities.get(nodeId), w);
                        return true;
                    });
                }
            );
            DoubleAdder weightToDivide = new DoubleAdder();
            rootGraph.forEachNode(nodeId -> {
                weightToDivide.add(nodeVolumes.get(nodeId));
                return true;
            });
            return 1.0 / weightToDivide.doubleValue();
        } else {
            nodeVolumes.setAll(rootGraph::degree);
            rootGraph.forEachNode(nodeId -> {
                long communityId = seedCommunities.get(nodeId);
                communityVolumes.addTo(communityId, rootGraph.degree(nodeId));
                return true;
            });
            return 1.0 / rootGraph.relationshipCount();
        }
    }

    private DendrogramResult buildDendrogram(
        Graph workingGraph,
        @Nullable HugeLongArray previousIterationDendrogram,
        HugeLongArray currentCommunities
    ) {

        assert workingGraph.nodeCount() == currentCommunities.size() : "The sizes of the graph and communities should match";

        var dendrogram = HugeLongArray.newArray(rootGraph.nodeCount());
        AtomicLong maxCommunityId = new AtomicLong(0L);
        ParallelUtil.parallelForEachNode(rootGraph, concurrency, (nodeId) -> {
            long prevId = previousIterationDendrogram == null
                ? nodeId
                : workingGraph.toMappedNodeId(previousIterationDendrogram.get(nodeId));

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

            dendrogram.set(nodeId, communityId);
        });

        return ImmutableDendrogramResult.of(maxCommunityId.get(), dendrogram);
    }

    @ValueClass
    interface DendrogramResult {
        long maxCommunityId();

        HugeLongArray dendrogram();
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
            long refinedCommunityId = workingGraph.toOriginalNodeId(aggregatedCommunityId);
            long localPhaseCommunityId = localPhaseCommunities.get(refinedCommunityId);
            long aggregatedSeedCommunityId;

            // cache the `aggregatedSeedCommunityId`
            if (localPhaseCommunityToAggregatedNewId.containsKey(localPhaseCommunityId)) {
                aggregatedSeedCommunityId = localPhaseCommunityToAggregatedNewId.get(localPhaseCommunityId);
            } else {
                aggregatedSeedCommunityId = aggregatedCommunityId;
                localPhaseCommunityToAggregatedNewId.put(localPhaseCommunityId, aggregatedSeedCommunityId);
            }

            double volumeOfTheAggregatedCommunity = refinedCommunityVolumes.get(refinedCommunityId);
            aggregatedCommunitySeedVolume.addTo(aggregatedSeedCommunityId, volumeOfTheAggregatedCommunity);

            seededCommunitiesForNextIteration.set(aggregatedCommunityId, aggregatedSeedCommunityId);
            aggregatedNodeSeedVolume.set(aggregatedCommunityId, volumeOfTheAggregatedCommunity);
            return true;
        });
        return new CommunityData(
            seededCommunitiesForNextIteration,
            aggregatedCommunitySeedVolume,
            aggregatedNodeSeedVolume,
            localPhaseCommunityToAggregatedNewId.size()
        );
    }

    @Override
    public void release() {

    }

    private double[] resizeModularitiesArray(int iteration) {
        double[] resizedModularities = new double[iteration];
        if (iteration < maxIterations) {
            System.arraycopy(this.modularities, 0, resizedModularities, 0, iteration);
        } else {
            return modularities;
        }
        return resizedModularities;
    }
}
