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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.collections.HugeSparseDoubleArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
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
        this.communityProperties= graph.nodeProperties(config.communityProperty());
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
        var tasks = PartitionUtils.degreePartition(
            graph,
            config.concurrency(),
            partition -> new CountRelationships(graph.concurrentCopy(), partition),
            Optional.of(config.minBatchSize())
        );
        ParallelUtil.runWithConcurrency(config.concurrency(), tasks, executor);

        var internalCountsBuilder = HugeSparseDoubleArray.builder(0.0D, allocationTracker::add);
        var externalCountsBuilder = HugeSparseDoubleArray.builder(0.0D, allocationTracker::add);

        tasks.forEach(countRelationships -> {
            var localInternalCounts = countRelationships.internalCounts();
            for (long community = 0; community < localInternalCounts.capacity(); community++) {
                internalCountsBuilder.addTo(community, localInternalCounts.get(community));
            }

            var localExternalCounts = countRelationships.externalCounts();
            for (long community = 0; community < localExternalCounts.capacity(); community++) {
                externalCountsBuilder.addTo(community, localExternalCounts.get(community));
            }
        });

        var internalCounts = internalCountsBuilder.build();
        var externalCounts = externalCountsBuilder.build();

        var conductancesBuilder = HugeSparseDoubleArray.builder(0.0D, allocationTracker::add);

        for (long community = 0; community < internalCounts.capacity(); community++) {
            var externalCount = externalCounts.get(community);
            conductancesBuilder.set(community, externalCount / (externalCount + internalCounts.get(community)));
        }

        return Result.of(conductancesBuilder.build(), 0.0D);
    }

    private final class CountRelationships implements Runnable {

        private final Graph graph;
        private HugeSparseDoubleArray internalCounts;
        private HugeSparseDoubleArray externalCounts;
        private final HugeSparseDoubleArray.Builder internalCountsBuilder = HugeSparseDoubleArray.builder(0.0D, allocationTracker::add);
        private final HugeSparseDoubleArray.Builder externalCountsBuilder = HugeSparseDoubleArray.builder(0.0D, allocationTracker::add);
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
