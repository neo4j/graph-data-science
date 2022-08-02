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
package org.neo4j.gds.modularity;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.api.properties.nodes.LongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.beta.k1coloring.ImmutableK1ColoringStreamConfig;
import org.neo4j.gds.beta.k1coloring.K1Coloring;
import org.neo4j.gds.beta.k1coloring.K1ColoringFactory;
import org.neo4j.gds.beta.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * Implementation of parallel modularity optimization based on:
 *
 * Lu, Hao, Mahantesh Halappanavar, and Ananth Kalyanaraman.
 * "Parallel heuristics for scalable community detection."
 * Parallel Computing 47 (2015): 19-37.
 * https://arxiv.org/pdf/1410.1237.pdf
 */
public final class ModularityOptimization extends Algorithm<ModularityOptimization> {

    public static final int K1COLORING_MAX_ITERATIONS = 5;
    private final int concurrency;
    private final int maxIterations;
    private final long nodeCount;
    private final long minBatchSize;
    private final double tolerance;
    private final Graph graph;
    private final NodePropertyValues seedProperty;
    private final ExecutorService executor;

    private final ModularityManager modularityManager;

    private int iterationCounter;
    private boolean didConverge = false;
    private double totalNodeWeight = 0.0;
    private double modularity = -1.0;
    private BitSet colorsUsed;
    private HugeLongArray colors;
    private HugeLongArray currentCommunities;
    private HugeLongArray nextCommunities;
    private HugeLongArray reverseSeedCommunityMapping;
    private HugeDoubleArray cumulativeNodeWeights;
    private HugeAtomicDoubleArray communityWeightUpdates;

    public ModularityOptimization(
        final Graph graph,
        int maxIterations,
        double tolerance,
        @Nullable NodePropertyValues seedProperty,
        int concurrency,
        int minBatchSize,
        ExecutorService executor,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.maxIterations = maxIterations;
        this.tolerance = tolerance;
        this.seedProperty = seedProperty;
        this.executor = executor;
        this.concurrency = concurrency;
        this.minBatchSize = minBatchSize;
        if (maxIterations < 1) {
            throw new IllegalArgumentException(formatWithLocale(
                "Need to run at least one iteration, but got %d",
                maxIterations
            ));
        }

        this.modularityManager = ModularityManager.create(graph, concurrency);
    }

    @Override
    public ModularityOptimization compute() {
        progressTracker.beginSubTask();


        progressTracker.beginSubTask();
        computeColoring();
        initSeeding();
        init();
        progressTracker.endSubTask();


        progressTracker.beginSubTask();
        for (iterationCounter = 0; iterationCounter < maxIterations; iterationCounter++) {
            progressTracker.beginSubTask();

            boolean hasConverged;

            long currentColor = colorsUsed.nextSetBit(0);
            while (currentColor != -1) {
                terminationFlag.assertRunning();
                optimizeForColor(currentColor);
                currentColor = colorsUsed.nextSetBit(currentColor + 1);
            }

            hasConverged = !updateModularity();

            progressTracker.endSubTask();

            if (hasConverged) {
                this.didConverge = true;
                iterationCounter++;
                break;
            }
        }
        progressTracker.endSubTask();

        progressTracker.endSubTask();
        return this;
    }

    private void computeColoring() {
        K1ColoringStreamConfig k1Config = ImmutableK1ColoringStreamConfig
            .builder()
            .concurrency(concurrency)
            .maxIterations(K1COLORING_MAX_ITERATIONS)
            .batchSize((int) minBatchSize)
            .build();

        K1Coloring coloring = new K1ColoringFactory<>().build(graph, k1Config, progressTracker);
        coloring.setTerminationFlag(terminationFlag);

        this.colors = coloring.compute();
        this.colorsUsed = coloring.usedColors();
    }

    private void initSeeding() {
        this.currentCommunities = HugeLongArray.newArray(nodeCount);

        if (seedProperty == null) {
            return;
        }

        long maxSeedCommunity = seedProperty.getMaxLongPropertyValue().orElse(0L);

        HugeLongLongMap communityMapping = new HugeLongLongMap(nodeCount);
        long nextAvailableInternalCommunityId = -1;

        for (long nodeId = 0; nodeId < nodeCount; nodeId++) {
            long seedCommunity = seedProperty.longValue(nodeId);
            if (seedCommunity < 0) {
                seedCommunity = -1;
            }

            seedCommunity = seedCommunity >= 0 ? seedCommunity : graph.toOriginalNodeId(nodeId) + maxSeedCommunity;
            if (communityMapping.getOrDefault(seedCommunity, -1) < 0) {
                communityMapping.addTo(seedCommunity, ++nextAvailableInternalCommunityId);
            }

            currentCommunities.set(nodeId, communityMapping.getOrDefault(seedCommunity, -1));
        }

        this.reverseSeedCommunityMapping = HugeLongArray.newArray(communityMapping.size());

        for (LongLongCursor entry : communityMapping) {
            reverseSeedCommunityMapping.set(entry.value, entry.key);
        }
    }

    private void init() {
        this.nextCommunities = HugeLongArray.newArray(nodeCount);
        this.cumulativeNodeWeights = HugeDoubleArray.newArray(nodeCount);

        this.communityWeightUpdates = HugeAtomicDoubleArray.newArray(nodeCount);

        var initTasks = PartitionUtils.rangePartition(concurrency, nodeCount, (partition) ->
            new InitTask(
                graph.concurrentCopy(),
                currentCommunities,
                modularityManager,
                cumulativeNodeWeights,
                seedProperty != null,
                partition
            ),
            Optional.of((int) minBatchSize));

        ParallelUtil.run(initTasks, executor);

        totalNodeWeight = initTasks.stream().mapToDouble(InitTask::localSum).sum();
        currentCommunities.copyTo(nextCommunities, nodeCount);
        modularityManager.totalWeight(totalNodeWeight);
    }

    private static final class InitTask implements Runnable {

        private final RelationshipIterator relationshipIterator;

        private final HugeLongArray currentCommunities;

        ModularityManager modularityManager;

        private final HugeDoubleArray cumulativeNodeWeights;

        private final boolean isSeeded;

        private final Partition partition;

        private double localSum;

        private InitTask(
            RelationshipIterator relationshipIterator,
            HugeLongArray currentCommunities,
            ModularityManager modularityManager,
            HugeDoubleArray cumulativeNodeWeights,
            boolean isSeeded,
            Partition partition
        ) {
            this.relationshipIterator = relationshipIterator;
            this.currentCommunities = currentCommunities;
            this.modularityManager = modularityManager;
            this.cumulativeNodeWeights = cumulativeNodeWeights;
            this.isSeeded = isSeeded;
            this.partition = partition;
            this.localSum = 0.0D;
        }

        @Override
        public void run() {
            var cumulativeWeight = new MutableDouble();

            partition.consume(nodeId -> {
                if (!isSeeded) {
                    currentCommunities.set(nodeId, nodeId);
                }

                cumulativeWeight.setValue(0.0D);
                relationshipIterator.forEachRelationship(nodeId, 1.0, (s, t, w) -> {
                    cumulativeWeight.add(w);
                    return true;
                });

                modularityManager.communityWeightUpdate(
                    currentCommunities.get(nodeId),
                    cumulativeWeight.doubleValue()
                );


                cumulativeNodeWeights.set(nodeId, cumulativeWeight.doubleValue());

                localSum += cumulativeWeight.doubleValue();
            });
        }

        double localSum() {
            return localSum;
        }
    }

    private void optimizeForColor(long currentColor) {
        // run optimization tasks for every node
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(createModularityOptimizationTasks(currentColor))
            .executor(executor)
            .run();

        // swap old and new communities
        nextCommunities.copyTo(currentCommunities, nodeCount);

        // apply communityWeight updates to communityWeights
        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, nodeCount),
            concurrency,
            stream -> stream.forEach(nodeId -> {
                final double update = communityWeightUpdates.get(nodeId);
                modularityManager.communityWeightUpdate(nodeId, update);
            })
        );

        // reset communityWeightUpdates
        communityWeightUpdates = HugeAtomicDoubleArray.newArray(nodeCount);
    }

    private Collection<ModularityOptimizationTask> createModularityOptimizationTasks(long currentColor) {
        return PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> new ModularityOptimizationTask(
                graph,
                partition,
                currentColor,
                totalNodeWeight,
                colors,
                currentCommunities,
                nextCommunities,
                cumulativeNodeWeights,
                communityWeightUpdates,
                modularityManager,
                progressTracker
            ),
            Optional.of((int) minBatchSize)
        );
    }

    private boolean updateModularity() {
        double oldModularity = this.modularity;
        this.modularity = calculateModularity();

        // We have nothing to compare against in the first iteration => the modularity was updated.
        if (iterationCounter == 0) {
            return true;
        }

        return this.modularity > oldModularity && Math.abs(this.modularity - oldModularity) > tolerance;
    }

    private double calculateModularity() {
        modularityManager.registerCommunities(currentCommunities);
        return modularityManager.calculateModularity();
    }

    @Override
    public void release() {
        this.nextCommunities.release();
        this.communityWeightUpdates.release();
        this.cumulativeNodeWeights.release();
        this.colors.release();
        this.colorsUsed = null;
    }

    public long getCommunityId(long nodeId) {
        if (seedProperty == null || reverseSeedCommunityMapping == null) {
            return currentCommunities.get(nodeId);
        }
        return reverseSeedCommunityMapping.get(currentCommunities.get(nodeId));
    }

    public int getIterations() {
        return this.iterationCounter;
    }

    public double getModularity() {
        return this.modularity;
    }

    public boolean didConverge() {
        return this.didConverge;
    }

    public LongNodePropertyValues asNodeProperties() {
        return new LongNodePropertyValues() {
            @Override
            public long longValue(long nodeId) {
                return getCommunityId(nodeId);
            }

            @Override
            public long size() {
                return currentCommunities.size();
            }
        };
    }
}
