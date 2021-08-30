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
package org.neo4j.gds.beta.modularity;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.cursors.LongLongCursor;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.RelationshipIterator;
import org.neo4j.gds.api.nodeproperties.LongNodeProperties;
import org.neo4j.gds.beta.k1coloring.ImmutableK1ColoringStreamConfig;
import org.neo4j.gds.beta.k1coloring.K1Coloring;
import org.neo4j.gds.beta.k1coloring.K1ColoringFactory;
import org.neo4j.gds.beta.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
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
public final class ModularityOptimization extends Algorithm<ModularityOptimization, ModularityOptimization> {

    public static final int K1COLORING_MAX_ITERATIONS = 5;
    private final int concurrency;
    private final int maxIterations;
    private final long nodeCount;
    private final long minBatchSize;
    private final double tolerance;
    private final Graph graph;
    private final NodeProperties seedProperty;
    private final ExecutorService executor;
    private final AllocationTracker tracker;

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
    private HugeDoubleArray nodeCommunityInfluences;
    private HugeAtomicDoubleArray communityWeights;
    private HugeAtomicDoubleArray communityWeightUpdates;

    public ModularityOptimization(
        final Graph graph,
        int maxIterations,
        double tolerance,
        @Nullable NodeProperties seedProperty,
        int concurrency,
        int minBatchSize,
        ExecutorService executor,
        ProgressTracker progressTracker,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.maxIterations = maxIterations;
        this.tolerance = tolerance;
        this.seedProperty = seedProperty;
        this.executor = executor;
        this.concurrency = concurrency;
        this.progressTracker = progressTracker;
        this.tracker = tracker;
        this.minBatchSize = minBatchSize;

        if (maxIterations < 1) {
            throw new IllegalArgumentException(formatWithLocale(
                "Need to run at least one iteration, but got %d",
                maxIterations
            ));
        }
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

            nodeCommunityInfluences.fill(0.0);

            long currentColor = colorsUsed.nextSetBit(0);
            while (currentColor != -1) {
                assertRunning();
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

        var progressLogger = progressTracker.progressLogger();
        K1Coloring coloring = new K1ColoringFactory<>()
            .build(graph, k1Config, tracker, progressLogger.getLog(), progressTracker.taskRegistry())
            .withTerminationFlag(terminationFlag);

        this.colors = coloring.compute();
        this.colorsUsed = coloring.usedColors();
    }

    private void initSeeding() {
        this.currentCommunities = HugeLongArray.newArray(nodeCount, tracker);

        if (seedProperty == null) {
            return;
        }

        long maxSeedCommunity = seedProperty.getMaxLongPropertyValue().orElse(0L);

        HugeLongLongMap communityMapping = new HugeLongLongMap(nodeCount, tracker);
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

        this.reverseSeedCommunityMapping = HugeLongArray.newArray(communityMapping.size(), tracker);

        for (LongLongCursor entry : communityMapping) {
            reverseSeedCommunityMapping.set(entry.value, entry.key);
        }
    }

    private void init() {
        this.nextCommunities = HugeLongArray.newArray(nodeCount, tracker);
        this.cumulativeNodeWeights = HugeDoubleArray.newArray(nodeCount, tracker);
        this.nodeCommunityInfluences = HugeDoubleArray.newArray(nodeCount, tracker);
        this.communityWeights = HugeAtomicDoubleArray.newArray(nodeCount, tracker);
        this.communityWeightUpdates = HugeAtomicDoubleArray.newArray(nodeCount, tracker);

        var initTasks = PartitionUtils.rangePartition(concurrency, nodeCount, (partition) ->
            new InitTask(
                graph.concurrentCopy(),
                currentCommunities,
                communityWeights,
                cumulativeNodeWeights,
                seedProperty != null,
                partition
            ),
            Optional.of((int) minBatchSize));

        ParallelUtil.run(initTasks, executor);

        var doubleTotalNodeWeight = initTasks.stream().mapToDouble(InitTask::localSum).sum();

        totalNodeWeight = doubleTotalNodeWeight / 2.0;
        currentCommunities.copyTo(nextCommunities, nodeCount);
    }

    private static final class InitTask implements Runnable {

        private final RelationshipIterator relationshipIterator;

        private final HugeLongArray currentCommunities;

        private final HugeAtomicDoubleArray communityWeights;

        private final HugeDoubleArray cumulativeNodeWeights;

        private final boolean isSeeded;

        private final Partition partition;

        private double localSum;

        private InitTask(
            RelationshipIterator relationshipIterator,
            HugeLongArray currentCommunities,
            HugeAtomicDoubleArray communityWeights,
            HugeDoubleArray cumulativeNodeWeights,
            boolean isSeeded,
            Partition partition
        ) {
            this.relationshipIterator = relationshipIterator;
            this.currentCommunities = currentCommunities;
            this.communityWeights = communityWeights;
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

                communityWeights.update(
                    currentCommunities.get(nodeId),
                    acc -> acc + cumulativeWeight.doubleValue()
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
        ParallelUtil.runWithConcurrency(
            concurrency,
            createModularityOptimizationTasks(currentColor),
            executor
        );

        // swap old and new communities
        nextCommunities.copyTo(currentCommunities, nodeCount);

        // apply communityWeight updates to communityWeights
        ParallelUtil.parallelStreamConsume(
            LongStream.range(0, nodeCount),
            concurrency,
            stream -> stream.forEach(nodeId -> {
                final double update = communityWeightUpdates.get(nodeId);
                communityWeights.update(nodeId, w -> w + update);
            })
        );

        // reset communityWeightUpdates
        communityWeightUpdates = HugeAtomicDoubleArray.newArray(nodeCount, tracker);
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
                nodeCommunityInfluences,
                communityWeights,
                communityWeightUpdates,
                progressTracker
            ),
            Optional.of((int) minBatchSize)
        );
    }

    private boolean updateModularity() {
        double oldModularity = this.modularity;
        this.modularity = calculateModularity();

        return this.modularity > oldModularity && Math.abs(this.modularity - oldModularity) > tolerance;
    }

    private double calculateModularity() {
        double ex = ParallelUtil.parallelStream(
            LongStream.range(0, nodeCount),
            concurrency,
            nodeStream ->
                nodeStream
                    .mapToDouble(nodeCommunityInfluences::get)
                    .reduce(Double::sum)
                    .orElseThrow(() -> new RuntimeException("Error while computing modularity"))
        );

        double ax = ParallelUtil.parallelStream(
            LongStream.range(0, nodeCount),
            concurrency,
            nodeStream ->
                nodeStream
                    .mapToDouble(nodeId -> Math.pow(communityWeights.get(nodeId), 2.0))
                    .reduce(Double::sum)
                    .orElseThrow(() -> new RuntimeException("Error while computing modularity"))
        );

        return (ex / (2 * totalNodeWeight)) - (ax / (Math.pow(2 * totalNodeWeight, 2)));
    }

    @Override
    public ModularityOptimization me() {
        return this;
    }

    @Override
    public void release() {
        this.nextCommunities.release();
        this.communityWeights.release();
        this.communityWeightUpdates.release();
        this.cumulativeNodeWeights.release();
        this.nodeCommunityInfluences.release();
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

    public LongNodeProperties asNodeProperties() {
        return new LongNodeProperties() {
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
