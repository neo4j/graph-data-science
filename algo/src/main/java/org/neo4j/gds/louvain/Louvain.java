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
import org.neo4j.gds.Orientation;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.beta.modularity.ImmutableModularityOptimizationStreamConfig;
import org.neo4j.gds.beta.modularity.ModularityOptimization;
import org.neo4j.gds.beta.modularity.ModularityOptimizationFactory;
import org.neo4j.gds.beta.modularity.ModularityOptimizationStreamConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsBuilder;
import org.neo4j.gds.core.utils.OriginalIdNodePropertyValues;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.gds.core.concurrency.ParallelUtil.DEFAULT_BATCH_SIZE;

public final class Louvain extends Algorithm<Louvain> {

    private final Graph rootGraph;
    private final LouvainBaseConfig config;
    private final NodePropertyValues seedingValues;
    private final ExecutorService executorService;
    // results
    private HugeLongArray[] dendrograms;
    private double[] modularities;
    private int ranLevels;

    public Louvain(
        Graph graph,
        LouvainBaseConfig config,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.config = config;
        this.rootGraph = graph;
        this.seedingValues = Optional.ofNullable(config.seedProperty()).map(graph::nodeProperties).orElse(null);
        this.executorService = executorService;
        this.dendrograms = new HugeLongArray[config.maxLevels()];
        this.modularities = new double[config.maxLevels()];
    }

    @Override
    public Louvain compute() {
        progressTracker.beginSubTask();

        Graph workingGraph = rootGraph;
        NodePropertyValues nextSeedingValues = seedingValues;

        long oldNodeCount = rootGraph.nodeCount();
        for (ranLevels = 0; ranLevels < config.maxLevels(); ranLevels++) {

            terminationFlag.assertRunning();

            ModularityOptimization modularityOptimization = runModularityOptimization(
                workingGraph,
                nextSeedingValues
            );
            modularityOptimization.release();

            modularities[ranLevels] = modularityOptimization.getModularity();
            dendrograms[ranLevels] = HugeLongArray.newArray(rootGraph.nodeCount());
            long maxCommunityId = buildDendrogram(workingGraph, ranLevels, modularityOptimization);

            workingGraph = summarizeGraph(workingGraph, modularityOptimization, maxCommunityId);
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
                resizeResultArrays();
                break;
            }
            oldNodeCount = workingGraph.nodeCount();
        }

        progressTracker.endSubTask();
        return this;
    }

    private void resizeResultArrays() {
        int numLevels = levels();
        HugeLongArray[] resizedDendrogram = new HugeLongArray[numLevels];
        double[] resizedModularities = new double[numLevels];
        if (numLevels < this.dendrograms.length) {
            System.arraycopy(this.dendrograms, 0, resizedDendrogram, 0, numLevels);
            System.arraycopy(this.modularities, 0, resizedModularities, 0, numLevels);
        }
        this.dendrograms = resizedDendrogram;
        this.modularities = resizedModularities;
    }

    private long buildDendrogram(
        Graph workingGraph,
        int level,
        ModularityOptimization modularityOptimization
    ) {
        AtomicLong maxCommunityId = new AtomicLong(0L);
        ParallelUtil.parallelForEachNode(rootGraph, config.concurrency(), (nodeId) -> {
            long prevId = level == 0
                ? nodeId
                : workingGraph.toMappedNodeId(dendrograms[level - 1].get(nodeId));

            long communityId = modularityOptimization.getCommunityId(prevId);

            boolean updatedMaxCurrentId;
            do {
                var currentMaxId = maxCommunityId.get();
                if (communityId > currentMaxId) {
                    updatedMaxCurrentId = maxCommunityId.compareAndSet(currentMaxId, communityId);
                } else {
                    updatedMaxCurrentId = true;
                }
            } while (!updatedMaxCurrentId);

            dendrograms[level].set(nodeId, communityId);
        });

        return maxCommunityId.get();
    }

    private ModularityOptimization runModularityOptimization(Graph louvainGraph, NodePropertyValues seed) {
        ModularityOptimizationStreamConfig modularityOptimizationConfig = ImmutableModularityOptimizationStreamConfig
            .builder()
            .maxIterations(config.maxIterations())
            .tolerance(config.tolerance())
            .concurrency(config.concurrency())
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

        modularityOptimization.compute();

        return modularityOptimization;
    }

    private Graph summarizeGraph(
        Graph workingGraph,
        ModularityOptimization modularityOptimization,
        long maxCommunityId
    ) {
        var nodesBuilder = GraphFactory.initNodesBuilder()
            .maxOriginalId(maxCommunityId)
            .concurrency(config.concurrency())
            .build();

        terminationFlag.assertRunning();

        workingGraph.forEachNode((nodeId) -> {
            nodesBuilder.addNode(modularityOptimization.getCommunityId(nodeId));
            return true;
        });

        terminationFlag.assertRunning();
        double scaleCoefficient = 1.0;
        if (workingGraph.schema().isUndirected()) {
            scaleCoefficient /= 2.0;
        }
        Orientation orientation = rootGraph.schema().isUndirected() ? Orientation.UNDIRECTED : Orientation.NATURAL;
        IdMap idMap = nodesBuilder.build().idMap();
        RelationshipsBuilder relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(idMap)
            .orientation(orientation)
            .addPropertyConfig(Aggregation.SUM, DefaultValue.forDouble())
            .executorService(executorService)
            .build();

        double finalScaleCoefficient = scaleCoefficient;
        var relationshipCreators = PartitionUtils.rangePartition(
            config.concurrency(),
            workingGraph.nodeCount(),
            partition ->
                new RelationshipCreator(
                    relationshipsBuilder,
                    modularityOptimization,
                    workingGraph.concurrentCopy(),
                    finalScaleCoefficient,
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
        return !(currentModularity > previousModularity && Math.abs(currentModularity - previousModularity) > config.tolerance());
    }

    public HugeLongArray[] dendrograms() {
        return this.dendrograms;
    }

    public HugeLongArray finalDendrogram() {
        return this.dendrograms[levels() - 1];
    }

    public long getCommunity(long nodeId) {
        return dendrograms[levels() - 1].get(nodeId);
    }

    public long[] getCommunities(long nodeId) {
        long[] communities = new long[dendrograms.length];

        for (int i = 0; i < dendrograms.length; i++) {
            communities[i] = dendrograms[i].get(nodeId);
        }

        return communities;
    }

    public int levels() {
        return this.ranLevels == 0 ? 1 : this.ranLevels;
    }

    public double[] modularities() {
        return this.modularities;
    }

    @Override
    public void release() {
        this.rootGraph.releaseTopology();
    }

    static final class RelationshipCreator implements Runnable {

        private final RelationshipsBuilder relationshipsBuilder;

        private final ModularityOptimization modularityOptimization;

        private final RelationshipIterator relationshipIterator;

        private final Partition partition;

        private final double scaleCoefficient;


        private RelationshipCreator(
            RelationshipsBuilder relationshipsBuilder,
            ModularityOptimization modularityOptimization,
            RelationshipIterator relationshipIterator,
            double scaleCoefficient,
            Partition partition
        ) {
            this.relationshipsBuilder = relationshipsBuilder;
            this.modularityOptimization = modularityOptimization;
            this.relationshipIterator = relationshipIterator;
            this.partition = partition;
            this.scaleCoefficient = scaleCoefficient;
        }

        @Override
        public void run() {
            partition.consume(nodeId -> {
                long communityId = modularityOptimization.getCommunityId(nodeId);
                relationshipIterator.forEachRelationship(nodeId, 1.0, (source, target, property) -> {
                    if (source == target) {
                        relationshipsBuilder.add(communityId, modularityOptimization.getCommunityId(target), property);
                    } else {
                        relationshipsBuilder.add(
                            communityId,
                            modularityOptimization.getCommunityId(target),
                            property * scaleCoefficient
                        );

                    }

                    return true;
                });
            });
        }
    }
}
