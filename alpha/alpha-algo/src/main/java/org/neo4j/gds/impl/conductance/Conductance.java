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
package org.neo4j.gds.impl.conductance;

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.collections.HugeSparseDoubleArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.AtomicDoubleArray;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class Conductance extends Algorithm<Conductance, Conductance.Result> {

    private static final double DEFAULT_WEIGHT = 0.0D;

    private Graph graph;
    private final ExecutorService executor;
    private final ConductanceConfig config;
    private final AllocationTracker allocationTracker;
    private final WeightTransformer weightTransformer;
    private final NodeProperties communityProperties;

    public Conductance(
        Graph graph,
        ExecutorService executor,
        ConductanceConfig config,
        ProgressTracker progressTracker,
        AllocationTracker allocationTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.executor = executor;
        this.config = config;
        this.allocationTracker = allocationTracker;
        this.weightTransformer = config.hasRelationshipWeightProperty() ? weight -> weight : unused -> 1.0D;
        this.communityProperties = graph.nodeProperties(config.communityProperty());
    }

    @FunctionalInterface
    private interface WeightTransformer {
        double accept(double weight);
    }

    @ValueClass
    public interface Result {
        HugeSparseDoubleArray communityConductances();

        double globalConductance();

        static Result of(
            HugeSparseDoubleArray communityConductances,
            double globalConductance
        ) {
            return ImmutableResult
                .builder()
                .communityConductances(communityConductances)
                .globalConductance(globalConductance)
                .build();
        }
    }

    @Override
    public Result compute() {
        var countTasks = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> new CountRelationships(graph.concurrentCopy(), partition),
            Optional.of(config.minBatchSize())
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), countTasks, executor);

        var maxCapacity = new MutableLong(0);
        countTasks.forEach(countRelationships -> {
            long maxLocalCapacity = Math.max(countRelationships.internalCounts().capacity(), countRelationships.externalCounts().capacity());
            if (maxLocalCapacity > maxCapacity.longValue()) {
                maxCapacity.setValue(maxLocalCapacity);
            }
        });
        long batchSize = maxCapacity.longValue() / config.concurrency();
        long remainder = Math.floorMod(maxCapacity.longValue(), config.concurrency());

        var internalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            maxCapacity.longValue(),
            allocationTracker::add
        );
        var externalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            maxCapacity.longValue(),
            allocationTracker::add
        );

        var accumulateTasks = ParallelUtil.tasks(config.concurrency(), index -> () -> {
            final long startOffset = index * batchSize;
            final long endOffset = index == config.concurrency() - 1
                ? startOffset + batchSize + remainder
                : startOffset + batchSize;

            countTasks.forEach(countRelationships -> {
                var localInternalCounts = countRelationships.internalCounts();
                for (long community = startOffset; community < Math.min(endOffset, localInternalCounts.capacity()); community++) {
                    double localInternalCount = localInternalCounts.get(community);
                    if (Double.isNaN(localInternalCount)) {
                        continue;
                    }
                    internalCountsBuilder.setIfAbsent(community, 0.0D);
                    internalCountsBuilder.addTo(community, localInternalCounts.get(community));
                }

                var localExternalCounts = countRelationships.externalCounts();
                for (long community = startOffset; community < Math.min(endOffset, localExternalCounts.capacity()); community++) {
                    double localExternalCount = localExternalCounts.get(community);
                    if (Double.isNaN(localExternalCount)) {
                        continue;
                    }
                    externalCountsBuilder.setIfAbsent(community, 0.0D);
                    externalCountsBuilder.addTo(community, localExternalCounts.get(community));
                }
            });
        });
        ParallelUtil.runWithConcurrency(config.concurrency(), accumulateTasks, Pools.DEFAULT);

        var internalCounts = internalCountsBuilder.build();
        var externalCounts = externalCountsBuilder.build();

        var conductancesBuilder = HugeSparseDoubleArray.builder(Double.NaN, maxCapacity.longValue(), allocationTracker::add);
        var globalConductance = new AtomicDoubleArray(1);

        var computeTasks = ParallelUtil.tasks(config.concurrency(), index -> () -> {
            final long startOffset = index * batchSize;
            final long endOffset = index == config.concurrency() - 1
                ? startOffset + batchSize + remainder
                : startOffset + batchSize;
            double threadGlobalConductance = 0.0;

            for (long community = startOffset; community < Math.min(endOffset, internalCounts.capacity()); community++) {
                double internalCount = internalCounts.get(community);
                double externalCount = externalCounts.get(community);
                if (Double.isNaN(internalCount) || Double.isNaN(externalCount)) {
                    continue;
                }

                double localConductance = externalCount / (externalCount + internalCount);
                conductancesBuilder.set(community, localConductance);
                threadGlobalConductance += localConductance;
            }

            globalConductance.add(0, threadGlobalConductance);
        });
        ParallelUtil.runWithConcurrency(config.concurrency(), computeTasks, Pools.DEFAULT);

        return Result.of(conductancesBuilder.build(), globalConductance.get(0));
    }

    private final class CountRelationships implements Runnable {

        private final Graph graph;
        private HugeSparseDoubleArray internalCounts;
        private HugeSparseDoubleArray externalCounts;
        private final HugeSparseDoubleArray.Builder internalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            allocationTracker::add
        );
        private final HugeSparseDoubleArray.Builder externalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            allocationTracker::add
        );
        private final Partition partition;

        CountRelationships(
            Graph graph,
            Partition partition
        ) {
            this.graph = graph;
            this.partition = partition;
        }

        @Override
        public void run() {
            partition.consume(nodeId -> {
                long sourceCommunity = communityProperties.longValue(nodeId);

                graph.forEachRelationship(
                    nodeId,
                    DEFAULT_WEIGHT,
                    (sourceNodeId, targetNodeId, weight) -> {
                        long targetCommunity = communityProperties.longValue(targetNodeId);

                        internalCountsBuilder.setIfAbsent(sourceCommunity, 0.0D);
                        externalCountsBuilder.setIfAbsent(sourceCommunity, 0.0D);

                        if (sourceCommunity == targetCommunity) {
                            internalCountsBuilder.addTo(sourceCommunity, weightTransformer.accept(weight));
                        } else {
                            externalCountsBuilder.addTo(sourceCommunity, weightTransformer.accept(weight));
                        }

                        return true;
                    }
                );
            });

            internalCounts = internalCountsBuilder.build();
            externalCounts = externalCountsBuilder.build();

            progressTracker.logProgress(partition.nodeCount());
        }


        public HugeSparseDoubleArray internalCounts() {
            return internalCounts;
        }

        public HugeSparseDoubleArray externalCounts() {
            return externalCounts;
        }
    }

    @Override
    public Conductance me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }
}
