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
package org.neo4j.graphalgo.beta.pregel;

import org.immutables.value.Value;
import org.jetbrains.annotations.NotNull;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.context.MasterComputeContext;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.partition.Partition;
import org.neo4j.graphalgo.core.utils.partition.PartitionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PUBLIC, depluralize = true, deepImmutablesDetection = true)
public final class Pregel<CONFIG extends PregelConfig> {

    private final CONFIG config;

    private final PregelComputation<CONFIG> computation;

    private final Graph graph;

    private final NodeValue nodeValues;

    private final Messenger<?> messenger;

    private final int concurrency;
    private final ExecutorService executor;
    private final AllocationTracker tracker;

    public static <CONFIG extends PregelConfig> Pregel<CONFIG> create(
        Graph graph,
        CONFIG config,
        PregelComputation<CONFIG> computation,
        ExecutorService executor,
        AllocationTracker tracker
    ) {
        // This prevents users from disabling concurrency
        // validation in custom PregelConfig implementations.
        // Creating a copy of the user config triggers the
        // concurrency validations.
        ImmutablePregelConfig.copyOf(config);

        return new Pregel<>(
            graph,
            config,
            computation,
            NodeValue.of(computation.schema(config), graph.nodeCount(), config.concurrency(), tracker),
            executor,
            tracker
        );
    }

    public static MemoryEstimation memoryEstimation(PregelSchema pregelSchema, boolean isQueueBased, boolean isAsync) {
        var estimationBuilder = MemoryEstimations.builder(Pregel.class)
            .perNode("vote bits", MemoryUsage::sizeOfHugeAtomicBitset)
            .perThread("compute steps", MemoryEstimations.builder(ComputeStep.class).build())
            .add("node value", NodeValue.memoryEstimation(pregelSchema));

        if (isQueueBased) {
            if (isAsync) {
                estimationBuilder.add("message queues", AsyncQueueMessenger.memoryEstimation());
            } else {
                estimationBuilder.add("message queues", SyncQueueMessenger.memoryEstimation());
            }
        } else {
            estimationBuilder.add("message arrays", ReducingMessenger.memoryEstimation());
        }

        return estimationBuilder.build();
    }

    private Pregel(
        final Graph graph,
        final CONFIG config,
        final PregelComputation<CONFIG> computation,
        final NodeValue initialNodeValue,
        final ExecutorService executor,
        final AllocationTracker tracker
    ) {
        this.graph = graph;
        this.config = config;
        this.computation = computation;
        this.nodeValues = initialNodeValue;
        this.concurrency = config.concurrency();
        this.executor = executor;
        this.tracker = tracker;

        var reducer = computation.reducer();

        this.messenger = reducer.isPresent()
            ? new ReducingMessenger(graph, config, reducer.get(), tracker)
            : config.isAsynchronous()
                ? new AsyncQueueMessenger(graph.nodeCount(), tracker)
                : new SyncQueueMessenger(graph.nodeCount(), tracker);
    }

    public PregelResult run() {
        boolean didConverge = false;
        // Tracks if a node voted to halt in the previous iteration
        HugeAtomicBitSet voteBits = HugeAtomicBitSet.create(graph.nodeCount(), tracker);

        var computeSteps = createComputeSteps(voteBits);

        int iterations;
        for (iterations = 0; iterations < config.maxIterations(); iterations++) {
            // Init compute steps with the updated state
            for (var computeStep : computeSteps) {
                computeStep.init(iterations);
            }

            // Init messenger with the updated state
            messenger.initIteration(iterations);

            // Run the computation
            runComputeSteps(computeSteps);
            runMasterComputeStep(iterations);


            var lastIterationSendMessages = computeSteps
                .stream()
                .anyMatch(ComputeStep::hasSendMessage);

            // No messages have been sent and all nodes voted to halt
            if (!lastIterationSendMessages && voteBits.allSet()) {
                didConverge = true;
                break;
            }
        }

        return ImmutablePregelResult.builder()
            .nodeValues(nodeValues)
            .didConverge(didConverge)
            .ranIterations(iterations)
            .build();
    }

    public void release() {
        messenger.release();
    }

    private List<ComputeStep<CONFIG, ?>> createComputeSteps(HugeAtomicBitSet voteBits) {

        List<Partition> partitions = partitionGraph();

        List<ComputeStep<CONFIG, ?>> computeSteps = new ArrayList<>(concurrency);

        for (Partition partition : partitions) {
            computeSteps.add(new ComputeStep<>(
                graph,
                computation,
                config,
                0,
                partition,
                nodeValues,
                messenger,
                voteBits,
                graph
            ));
        }
        return computeSteps;
    }

    @NotNull
    private List<Partition> partitionGraph() {
        switch (config.partitioning()) {
            case RANGE:
                return PartitionUtils.rangePartition(concurrency, graph.nodeCount());
            case DEGREE:
                var batchSize = Math.max(
                    ParallelUtil.DEFAULT_BATCH_SIZE,
                    BitUtil.ceilDiv(graph.relationshipCount(), concurrency)
                );
                return PartitionUtils.degreePartition(graph, batchSize);
            default:
                throw new IllegalArgumentException(formatWithLocale(
                    "Unsupported partitioning `%s`",
                    config.partitioning()
                ));
        }
    }

    private void runComputeSteps(Collection<ComputeStep<CONFIG, ?>> computeSteps) {
        ParallelUtil.runWithConcurrency(concurrency, computeSteps, executor);
    }

    private void runMasterComputeStep(int iteration) {
        var context = new MasterComputeContext<>(config, graph, iteration, nodeValues);
        computation.masterCompute(context);
    }

    @ValueClass
    public interface PregelResult {

        NodeValue nodeValues();

        int ranIterations();

        boolean didConverge();
    }
}
