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
package org.neo4j.gds.louvain;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.OriginalIdNodePropertyValues;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.modularityoptimization.ImmutableModularityOptimizationStreamConfig;
import org.neo4j.gds.modularityoptimization.ModularityOptimization;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationFactory;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationStreamConfig;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.gds.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;

public final class Louvain extends Algorithm<LouvainResult> {

    private final Graph rootGraph;
    private final NodePropertyValues seedingValues;
    private final ExecutorService executorService;
    // results
    private final LouvainDendrogramManager dendrogramManager;
    private double[] modularities;
    private int ranLevels;

    private final int maxLevels;

    private final int concurrency;

    private final int maxIterations;

    private final double tolerance;

    private final boolean trackIntermediateCommunities;

    public Louvain(
        Graph graph,
        LouvainBaseConfig config,
        boolean trackIntermediateCommunities,
        int maxLevels,
        int maxIterations,
        double tolerance,
        int concurrency,
        ProgressTracker progressTracker,
        ExecutorService executorService
    ) {
        super(progressTracker);
        this.rootGraph = graph;
        this.maxIterations = maxIterations;
        this.concurrency = concurrency;
        this.seedingValues = Optional.ofNullable(config.seedProperty()).map(graph::nodeProperties).orElse(null);
        this.executorService = executorService;
        this.dendrogramManager = new LouvainDendrogramManager(
            graph.nodeCount(),
            maxLevels,
            trackIntermediateCommunities
        );
        this.tolerance = tolerance;
        this.modularities = new double[maxLevels];
        this.maxLevels = maxLevels;
        this.trackIntermediateCommunities = trackIntermediateCommunities;
    }

    @Override
    public LouvainResult compute() {
        progressTracker.beginSubTask();

        Graph workingGraph = rootGraph;
        NodePropertyValues nextSeedingValues = seedingValues;

        boolean resized = false;

        long oldNodeCount = rootGraph.nodeCount();
        for (ranLevels = 0; ranLevels < maxLevels; ranLevels++) {

            terminationFlag.assertRunning();

            var modularityOptimizationResult = runModularityOptimization(
                workingGraph,
                nextSeedingValues
            );

            modularities[ranLevels] = modularityOptimizationResult.modularity();
            dendrogramManager.prepareNextLevel(ranLevels);

            long maxCommunityId = buildDendrogram(
                workingGraph,
                ranLevels,
                modularityOptimizationResult
            );

            workingGraph = summarizeGraph(workingGraph, modularityOptimizationResult, maxCommunityId);
            nextSeedingValues = new OriginalIdNodePropertyValues(workingGraph) {
                @Override
                public OptionalLong getMaxLongPropertyValue() {
                    // We want to use the maxSeedCommunity with value 0 in all subsequent iterations
                    return OptionalLong.empty();
                }
            };

            if (workingGraph.nodeCount() == oldNodeCount
                || workingGraph.nodeCount() == 1
                || hasConverged()
            ) {
                resized = true;
                resizeResultArrays();
                break;
            }
            oldNodeCount = workingGraph.nodeCount();
        }
        if (!resized && !trackIntermediateCommunities) {
            resizeResultArrays();
        }
        progressTracker.endSubTask();
        return LouvainResult.of(
            dendrogramManager.getCurrent(),
            levels(),
            dendrogramManager,
            modularities,
            modularities[levels() - 1]
        );
    }

    private void resizeResultArrays() {
        int numLevels = levels();
        double[] resizedModularities = new double[numLevels];
        if (numLevels < maxLevels) {
            System.arraycopy(this.modularities, 0, resizedModularities, 0, numLevels);
            this.modularities = resizedModularities;
        }
        dendrogramManager.resizeDendrogram(numLevels);

    }

    private long buildDendrogram(
        Graph workingGraph,
        int level,
        ModularityOptimizationResult modularityOptimizationResult
    ) {
        AtomicLong maxCommunityId = new AtomicLong(0L);
        ParallelUtil.parallelForEachNode(rootGraph.nodeCount(), concurrency, terminationFlag, nodeId -> {
            long prevId = level == 0
                ? nodeId
                : workingGraph.toMappedNodeId(dendrogramManager.getPrevious(nodeId));

            long communityId = modularityOptimizationResult.communityId(prevId);

            boolean updatedMaxCurrentId;
            do {
                var currentMaxId = maxCommunityId.get();
                if (communityId > currentMaxId) {
                    updatedMaxCurrentId = maxCommunityId.compareAndSet(currentMaxId, communityId);
                } else {
                    updatedMaxCurrentId = true;
                }
            } while (!updatedMaxCurrentId);

            dendrogramManager.set(nodeId, communityId);
        });

        return maxCommunityId.get();
    }

    private ModularityOptimizationResult runModularityOptimization(Graph louvainGraph, NodePropertyValues seed) {
        ModularityOptimizationStreamConfig modularityOptimizationConfig = ImmutableModularityOptimizationStreamConfig
            .builder()
            .maxIterations(maxIterations)
            .tolerance(tolerance)
            .concurrency(concurrency)
            .batchSize(DEFAULT_BATCH_SIZE)
            .build();

        ModularityOptimization modularityOptimization = new ModularityOptimizationFactory<>()
            .build(
                louvainGraph,
                modularityOptimizationConfig,
                seed,
                progressTracker
            );
        modularityOptimization.setTerminationFlag(terminationFlag);

        var modularityOptimizationResult = modularityOptimization.compute();
        return modularityOptimizationResult;
    }

    private Graph summarizeGraph(
        Graph workingGraph,
        ModularityOptimizationResult modularityOptimizationResult,
        long maxCommunityId
    ) {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(maxCommunityId)
            .concurrency(concurrency)
            .build();

        terminationFlag.assertRunning();

        workingGraph.forEachNode((nodeId) -> {
            nodesBuilder.addNode(modularityOptimizationResult.communityId(nodeId));
            return true;
        });

        terminationFlag.assertRunning();

        IdMap idMap = nodesBuilder.build().idMap();
        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .relationshipType(RelationshipType.of("IGNORED"))
            .orientation(rootGraph.schema().direction().toOrientation())
            .addPropertyConfig(GraphFactory.PropertyConfig.builder()
                .propertyKey("property")
                .aggregation(Aggregation.SUM)
                .build())
            .executorService(executorService)
            .build();

        var relationshipCreators = PartitionUtils.degreePartition(
            workingGraph,
            concurrency,
            partition ->
                new RelationshipCreator(
                    relationshipsBuilder,
                    modularityOptimizationResult,
                    workingGraph.concurrentCopy(),
                    partition
                ),
            Optional.empty()
        );

        ParallelUtil.run(relationshipCreators, executorService);

        return GraphFactory.create(idMap, relationshipsBuilder.build());
    }

    private boolean hasConverged() {
        if (ranLevels == 0) {
            return false;
        }

        double previousModularity = modularities[ranLevels - 1];
        double currentModularity = modularities[ranLevels];
        return !(currentModularity > previousModularity && Math.abs(currentModularity - previousModularity) > tolerance);
    }

    private int levels() {
        return this.ranLevels == 0 ? 1 : this.ranLevels;
    }

    static final class RelationshipCreator implements Runnable {

        private final RelationshipsBuilder relationshipsBuilder;

        private final ModularityOptimizationResult modularityOptimizationResult;

        private final RelationshipIterator relationshipIterator;

        private final Partition partition;

        private RelationshipCreator(
            RelationshipsBuilder relationshipsBuilder,
            ModularityOptimizationResult modularityOptimizationResult,
            RelationshipIterator relationshipIterator,
            Partition partition
        ) {
            this.relationshipsBuilder = relationshipsBuilder;
            this.modularityOptimizationResult = modularityOptimizationResult;
            this.relationshipIterator = relationshipIterator;
            this.partition = partition;
        }

        @Override
        public void run() {
            partition.consume(nodeId -> {
                long communityId = modularityOptimizationResult.communityId(nodeId);
                relationshipIterator.forEachRelationship(nodeId, 1.0, (source, target, property) -> {
                    //ignore scaling alltogether
                        relationshipsBuilder.add(
                            communityId,
                            modularityOptimizationResult.communityId(target),
                            property
                        );


                    return true;
                });
            });
        }
    }
}
