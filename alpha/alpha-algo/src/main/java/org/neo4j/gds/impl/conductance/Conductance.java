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
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.collections.HugeSparseDoubleArray;
import org.neo4j.gds.core.concurrency.AtomicDouble;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class Conductance extends Algorithm<Conductance.Result> {

    private static final double DEFAULT_WEIGHT = 0.0D;

    private Graph graph;
    private final ExecutorService executor;
    private final ConductanceConfig config;
    private final WeightTransformer weightTransformer;
    private final NodePropertyValues communityProperties;

    public Conductance(
        Graph graph,
        ExecutorService executor,
        ConductanceConfig config,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.executor = executor;
        this.config = config;
        this.weightTransformer = config.hasRelationshipWeightProperty() ? weight -> weight : unused -> 1.0D;
        this.communityProperties = graph.nodeProperties(config.communityProperty());
    }

    @FunctionalInterface
    private interface WeightTransformer {
        double accept(double weight);
    }

    @ValueClass
    public interface CommunityResult {
        long community();

        double conductance();

        static CommunityResult of(long community, double conductance) {
            return ImmutableCommunityResult
                .builder()
                .community(community)
                .conductance(conductance)
                .build();
        }
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

        default Stream<CommunityResult> streamCommunityResults() {
            var condunctances = communityConductances();

            return LongStream
                .range(0, condunctances.capacity())
                .filter(c -> !Double.isNaN(condunctances.get(c)))
                .mapToObj(c -> CommunityResult.of(c, condunctances.get(c)));
        }
    }

    @Override
    public Result compute() {
        progressTracker.beginSubTask();

        var relCountTasks = countRelationships();

        long maxCommunityId = maxCommunityId(relCountTasks);
        long communitiesPerBatch = maxCommunityId / config.concurrency();
        long communitiesRemainder = Math.floorMod(maxCommunityId, config.concurrency());

        var accumulatedCounts = accumulateCounts(communitiesPerBatch, communitiesRemainder, maxCommunityId, relCountTasks);

        var result = computeConductances(communitiesPerBatch, communitiesRemainder, maxCommunityId, accumulatedCounts);

        progressTracker.endSubTask();

        return result;
    }

    private List<CountRelationships> countRelationships() {
        progressTracker.beginSubTask();

        var tasks = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> new CountRelationships(graph.concurrentCopy(), partition),
            Optional.of(config.minBatchSize())
        );
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .executor(executor)
            .run();

        progressTracker.endSubTask();

        return tasks;
    }

    private long maxCommunityId(List<CountRelationships> relCountTasks) {
        var maxCommunityId = new MutableLong(0);

        relCountTasks.forEach(countRelationships -> {
            long maxLocalCapacity = Math.max(
                countRelationships.internalCounts().capacity(),
                countRelationships.externalCounts().capacity()
            );
            if (maxLocalCapacity > maxCommunityId.longValue()) {
                maxCommunityId.setValue(maxLocalCapacity);
            }
        });

        return maxCommunityId.longValue();
    }

    private RelationshipCounts accumulateCounts(
        long communitiesPerBatch,
        long communitiesRemainder,
        long maxCommunityId,
        List<CountRelationships> relCountTasks
    ) {
        progressTracker.beginSubTask(maxCommunityId);

        var internalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            maxCommunityId
        );
        var externalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            maxCommunityId
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

            progressTracker.logProgress(endOffset - startOffset);
        });

        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .run();

        progressTracker.endSubTask();

        return ImmutableRelationshipCounts.of(internalCountsBuilder.build(), externalCountsBuilder.build());
    }

    private Result computeConductances(
        long communitiesPerBatch,
        long communitiesRemainder,
        long maxCommunityId,
        RelationshipCounts relCounts
    ) {
        progressTracker.beginSubTask(maxCommunityId);

        var conductancesBuilder = HugeSparseDoubleArray.builder(
            Double.NaN,
            maxCommunityId
        );
        var globalConductanceSum = new AtomicDouble();
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

            globalConductanceSum.getAndAdd(conductanceSum);
            globalValidCommunities.addAndGet(validCommunities);

            progressTracker.logProgress(endOffset - startOffset);
        });

        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .run();

        progressTracker.endSubTask();

        return Result.of(conductancesBuilder.build(), globalConductanceSum.get() / globalValidCommunities.longValue());
    }

    private final class CountRelationships implements Runnable {

        private final Graph graph;
        private HugeSparseDoubleArray internalCounts;
        private HugeSparseDoubleArray externalCounts;
        private final HugeSparseDoubleArray.Builder internalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN
        );
        private final HugeSparseDoubleArray.Builder externalCountsBuilder = HugeSparseDoubleArray.builder(
            Double.NaN
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
                if (sourceCommunity < 0) {
                    // Only non-negative numbers represent valid communities.
                    return;
                }

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

    @ValueClass
    interface RelationshipCounts {
        HugeSparseDoubleArray internalCounts();

        HugeSparseDoubleArray externalCounts();
    }

}
