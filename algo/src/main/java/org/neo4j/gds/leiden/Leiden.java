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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
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
    private final Optional<NodePropertyValues> seedValues;
    private final ExecutorService executorService;
    private final int concurrency;
    private final long randomSeed;
    private SeedCommunityManager seedCommunityManager;

    private final double tolerance;

    public Leiden(
        Graph graph,
        int maxIterations,
        double initialGamma,
        double theta,
        boolean includeIntermediateCommunities,
        long randomSeed,
        @Nullable NodePropertyValues seedValues,
        double tolerance,
        int concurrency,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.rootGraph = graph;
        this.orientation = rootGraph.schema().isUndirected() ? Orientation.UNDIRECTED : Orientation.NATURAL;
        this.maxIterations = maxIterations;
        this.initialGamma = initialGamma;
        this.theta = theta;
        this.randomSeed = randomSeed;
        // TODO: Pass these two as parameters
        this.executorService = Pools.DEFAULT;
        this.concurrency = concurrency;
        this.dendrogramManager = new LeidenDendrogramManager(
            rootGraph,
            maxIterations,
            concurrency,
            includeIntermediateCommunities
        );
        this.seedValues = Optional.ofNullable(seedValues);
        this.modularities = new double[maxIterations];
        this.modularity = 0d;
        this.tolerance = tolerance;
    }

    @Override
    public LeidenResult compute() {
        var workingGraph = rootGraph;
        var nodeCount = workingGraph.nodeCount();
        progressTracker.beginSubTask("Leiden");
        var localMoveCommunities = LeidenUtils.createStartingCommunities(nodeCount, seedValues.orElse(null));

        seedCommunityManager = SeedCommunityManager.create(seedValues.isPresent(), localMoveCommunities);

        var communityCount = seedCommunityManager.communitiesCount();

        // volume -> the sum of the weights of a nodes outgoing relationships
        var localMoveNodeVolumes = HugeDoubleArray.newArray(nodeCount);
        // the sum of the node volume for all nodes in a community
        var localMoveCommunityVolumes = HugeDoubleArray.newArray(nodeCount);
        double modularityScaleCoefficient = initVolumes(
            localMoveNodeVolumes,
            localMoveCommunityVolumes,
            localMoveCommunities
        );

        double gamma = this.initialGamma * modularityScaleCoefficient;

        //currentActualCommunities keeps a mapping of nodes to the community they currently belong to
        //if no seeding is involved, these values can be considered correct output.
        //Otherwise, they depict the current state without caring consider seeding (i.e., let's say seed:42 is mapped to community 0
        // then  currentCommunities.get(x)=0 not 42 whereas final output should be 42.
        HugeLongArray currentActualCommunities = HugeLongArray.newArray(rootGraph.nodeCount());

        boolean didConverge = false;
        int iteration;
        progressTracker.beginSubTask("Iteration");

        for (iteration = 0; iteration < maxIterations; iteration++) {
            // 1. LOCAL MOVE PHASE - over the singleton localMoveCommunities

            progressTracker.beginSubTask("Local Move");
            var localMovePhase = LocalMovePhase.create(
                workingGraph,
                localMoveCommunities,
                localMoveNodeVolumes,
                localMoveCommunityVolumes,
                gamma,
                communityCount,
                concurrency
            );

            var communitiesCount = localMovePhase.run();
            boolean localPhaseConverged = communitiesCount == workingGraph.nodeCount() || localMovePhase.swaps == 0;

            progressTracker.endSubTask("Local Move");

            progressTracker.beginSubTask("Modularity Computation");
            updateModularity(
                workingGraph,
                localMoveCommunities,
                localMoveCommunityVolumes,
                modularityScaleCoefficient,
                gamma,
                localPhaseConverged,
                iteration
            );
            progressTracker.endSubTask("Modularity Computation");


            if (localPhaseConverged) {
                didConverge = true;
                break;
            }
            var toleranceStatus = getToleranceStatus(iteration);

            //if you deteriotate performance, exit and return previous itreation
            if (toleranceStatus == ToleranceStatus.DECREASE) {
                break;
            }
            dendrogramManager.updateOutputDendrogram(
                workingGraph,
                currentActualCommunities,
                localMoveCommunities,
                seedCommunityManager,
                iteration
            ); //write user's output

            if (toleranceStatus == ToleranceStatus.CONVERGED) {
                didConverge = true;
                modularity = modularities[iteration];
                iteration++;
                break;
            } //if little difference from previous iteration, keep and break

            if (iteration < maxIterations - 1) { //if there's no next iteration, skip refinement/graph aggregation

                // 2 REFINE
                progressTracker.beginSubTask("Refinement");
                var refinementPhase = RefinementPhase.create(
                    workingGraph,
                    localMoveCommunities,
                    localMoveNodeVolumes,
                    localMoveCommunityVolumes,
                    gamma,
                    theta,
                    randomSeed,
                    concurrency,
                    executorService,
                    progressTracker
                );
                var refinementPhaseResult = refinementPhase.run();
                var refinedCommunities = refinementPhaseResult.communities();
                var refinedCommunityVolumes = refinementPhaseResult.communityVolumes();
                var maximumRefinedCommunityId = refinementPhaseResult.maximumRefinedCommunityId();
                progressTracker.endSubTask("Refinement");

                progressTracker.beginSubTask("Aggregation");
                dendrogramManager.updateAlgorithmDendrogram(
                    workingGraph,
                    currentActualCommunities,
                    refinedCommunities,
                    iteration
                );  //update the actual communities with the refined ones

                // 3 CREATE NEW GRAPH
                var graphAggregationPhase = new GraphAggregationPhase(
                    workingGraph,
                    this.orientation,
                    refinedCommunities,
                    maximumRefinedCommunityId,
                    this.executorService,
                    this.concurrency,
                    this.terminationFlag
                );
                workingGraph = graphAggregationPhase.run();

                // Post-aggregate step: MAINTAIN PARTITION
                var communityData = maintainPartition(
                    workingGraph,
                    localMoveCommunities,
                    refinedCommunityVolumes
                );
                localMoveCommunities = communityData.seededCommunitiesForNextIteration;
                localMoveCommunityVolumes = communityData.communityVolumes;
                localMoveNodeVolumes = communityData.aggregatedNodeSeedVolume;
                communityCount = communityData.communityCount;
                progressTracker.endSubTask("Aggregation");
            }
            modularity = modularities[iteration];

        }
        progressTracker.endSubTask("Iteration");

        progressTracker.endSubTask("Leiden");

        return getLeidenResult(didConverge, iteration);
    }

    @NotNull
    private LeidenResult getLeidenResult(boolean didConverge, int iteration) {
        boolean seedIsOptimal = didConverge && seedValues.isPresent() && iteration == 0;
        if (seedIsOptimal) {
            var modularity = modularities[0];
            return LeidenResult.of(
                LeidenUtils.createSeedCommunities(rootGraph.nodeCount(), seedValues.orElse(null)),
                1,
                didConverge,
                null,
                new double[]{modularity},
                modularity
            );
        } else {
            return LeidenResult.of(
                dendrogramManager.getCurrent(),
                iteration,
                didConverge,
                dendrogramManager,
                resizeModularitiesArray(iteration),
                modularity
            );
        }
    }
    
    private boolean updateModularity(
        Graph workingGraph,
        HugeLongArray localMoveCommunities,
        HugeDoubleArray localMoveCommunityVolumes,
        double modularityScaleCoefficient,
        double gamma,
        boolean localPhaseConverged,
        int iteration
    ) {
        boolean seedIsOptimal = localPhaseConverged && seedValues.isPresent() && iteration == 0;
        boolean shouldCalculateModularity = !localPhaseConverged || seedIsOptimal;

        if (shouldCalculateModularity) {
            modularities[iteration] = ModularityComputer.compute(
                workingGraph,
                localMoveCommunities,
                localMoveCommunityVolumes,
                gamma,
                modularityScaleCoefficient,
                concurrency,
                executorService,
                progressTracker
            );
        }
        return seedIsOptimal;
    }

    private double initVolumes(
        HugeDoubleArray nodeVolumes,
        HugeDoubleArray communityVolumes,
        HugeLongArray initialCommunities
    ) {
        progressTracker.beginSubTask("Initialization");
        double totalVolume;
        var volumeAdder = new DoubleAdder();
        if (rootGraph.hasRelationshipProperty()) {
            List<InitVolumeTask> tasks = PartitionUtils.rangePartition(
                concurrency,
                rootGraph.nodeCount(),
                partition -> new InitVolumeTask(
                    rootGraph.concurrentCopy(),
                    nodeVolumes,
                    partition,
                    volumeAdder
                ),
                Optional.empty()
            );
            RunWithConcurrency.builder().
                concurrency(concurrency).
                tasks(tasks).
                executor(executorService)
                .run();
            totalVolume = volumeAdder.sum();
        } else {
            nodeVolumes.setAll(rootGraph::degree);
            totalVolume = rootGraph.relationshipCount();
        }
        rootGraph.forEachNode(nodeId -> {
            long communityId = initialCommunities.get(nodeId);
            progressTracker.logProgress();
            communityVolumes.addTo(communityId, rootGraph.degree(nodeId));
            return true;
        });
        progressTracker.endSubTask("Initialization");

        return 1 / totalVolume;
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

    private ToleranceStatus getToleranceStatus(int iteration) {
        if (iteration == 0) {
            return ToleranceStatus.CONTINUE;
        } else {
            var difference = modularities[iteration] - modularities[iteration - 1];
            if (difference < 0) {
                return ToleranceStatus.DECREASE;
            }
            return (Double.compare(difference, tolerance) < 0) ? ToleranceStatus.CONVERGED : ToleranceStatus.CONTINUE;
        }
    }

    private enum ToleranceStatus {
        CONVERGED, DECREASE, CONTINUE
    }
}
