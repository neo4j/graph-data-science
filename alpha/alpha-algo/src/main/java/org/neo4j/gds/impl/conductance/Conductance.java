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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

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

        double globalAverageConductance();

        static Result of(
            HugeSparseDoubleArray communityConductances,
            double globalAverageConductance
        ) {
            return ImmutableResult
                .builder()
                .communityConductances(communityConductances)
                .globalAverageConductance(globalAverageConductance)
                .build();
        }
    }

    @Override
    public Result compute() {
        var relCountTasks = countRelationships();

        long maxCommunity = maxCommunity(relCountTasks);
        long communitiesPerBatch = maxCommunity / config.concurrency();
        long communitiesRemainder = Math.floorMod(maxCommunity, config.concurrency());

        var accumulatedCounts = accumulateCounts(communitiesPerBatch, communitiesRemainder, maxCommunity, relCountTasks);

        return computeConductances(communitiesPerBatch, communitiesRemainder, maxCommunity, accumulatedCounts);
    }

    private List<CountRelationships> countRelationships() {
        var tasks = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> new CountRelationships(graph.concurrentCopy(), partition),
            Optional.of(config.minBatchSize())
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);

        return tasks;
    }

    private long maxCommunity(List<CountRelationships> relCountTasks) {
        var maxCommunity = new MutableLong(0);

        relCountTasks.forEach(countRelationships -> {
            long maxLocalCapacity = Math.max(
                countRelationships.internalCounts().capacity(),
                countRelationships.externalCounts().capacity()
            );
            if (maxLocalCapacity > maxCommunity.longValue()) {
                maxCommunity.setValue(maxLocalCapacity);
            }
        });

        return maxCommunity.longValue();
    }

    private RelationshipCounts accumulateCounts(
        long communitiesPerBatch,
        long communitiesRemainder,
        long maxCommunity,
        List<CountRelationships> relCountTasks
    ) {
        var internalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            maxCommunity,
            allocationTracker::add
        );
        var externalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            maxCommunity,
            allocationTracker::add
        );

        var tasks = ParallelUtil.tasks(config.concurrency(), index -> () -> {
            final long startOffset = index * communitiesPerBatch;
            final long endOffset = index == config.concurrency() - 1
                ? startOffset + communitiesPerBatch + communitiesRemainder
                : startOffset + communitiesPerBatch;

            relCountTasks.forEach(countRelationships -> {
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

        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, Pools.DEFAULT);

        return ImmutableRelationshipCounts.of(internalCountsBuilder.build(), externalCountsBuilder.build());
    }

    private Result computeConductances(
        long communitiesPerBatch,
        long communitiesRemainder,
        long maxCommunity,
        RelationshipCounts relCounts
    ) {
        var conductancesBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            maxCommunity,
            allocationTracker::add
        );
        var globalConductanceSum = new AtomicDoubleArray(1);
        var globalValidCommunities = new AtomicLong();
        var internalCounts = relCounts.internalCounts();
        var externalCounts = relCounts.externalCounts();

        var tasks = ParallelUtil.tasks(config.concurrency(), index -> () -> {
            final long startOffset = index * communitiesPerBatch;
            final long endOffset = index == config.concurrency() - 1
                ? startOffset + communitiesPerBatch + communitiesRemainder
                : startOffset + communitiesPerBatch;
            double conductanceSum = 0.0;
            long validCommunities = 0;

            for (long community = startOffset; community < Math.min(endOffset, internalCounts.capacity()); community++) {
                double internalCount = internalCounts.get(community);
                double externalCount = externalCounts.get(community);
                if (Double.isNaN(internalCount) || Double.isNaN(externalCount)) {
                    continue;
                }

                double localConductance = externalCount / (externalCount + internalCount);
                conductancesBuilder.set(community, localConductance);
                conductanceSum += localConductance;
                validCommunities += 1;
            }

            globalConductanceSum.add(0, conductanceSum);
            globalValidCommunities.addAndGet(validCommunities);
        });

        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, Pools.DEFAULT);

        return Result.of(conductancesBuilder.build(), globalConductanceSum.get(0) / globalValidCommunities.longValue());
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
        }


        public HugeSparseDoubleArray internalCounts() {
            return internalCounts;
        }

        public HugeSparseDoubleArray externalCounts() {
            return externalCounts;
        }
    }

    @ValueClass
    interface RelationshipCounts {
        HugeSparseDoubleArray internalCounts();

        HugeSparseDoubleArray externalCounts();
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
