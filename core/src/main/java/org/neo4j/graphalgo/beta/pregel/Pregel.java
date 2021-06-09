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
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.beta.pregel.context.MasterComputeContext;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;

import java.util.concurrent.ExecutorService;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

@Value.Style(builderVisibility = Value.Style.BuilderVisibility.PUBLIC, depluralize = true, deepImmutablesDetection = true)
public final class Pregel<CONFIG extends PregelConfig> {

    private final CONFIG config;

    private final PregelComputation<CONFIG> computation;

    private final Graph graph;

    private final NodeValue nodeValues;

    private final Messenger<?> messenger;

    private final PregelComputer<CONFIG> computer;

    private final ProgressLogger progressLogger;

    private final ExecutorService executor;

    public static <CONFIG extends PregelConfig> Pregel<CONFIG> create(
        Graph graph,
        CONFIG config,
        PregelComputation<CONFIG> computation,
        ExecutorService executor,
        AllocationTracker tracker,
        ProgressLogger progressLogger
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
            tracker,
            progressLogger
        );
    }

    public static MemoryEstimation memoryEstimation(PregelSchema pregelSchema, boolean isQueueBased, boolean isAsync) {
        var estimationBuilder = MemoryEstimations.builder(Pregel.class)
            .perNode("vote bits", HugeAtomicBitSet::memoryEstimation)
            .perThread("compute steps", MemoryEstimations.builder(PartitionedComputeStep.class).build())
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
        final AllocationTracker tracker,
        final ProgressLogger progressLogger
    ) {
        this.graph = graph;
        this.config = config;
        this.computation = computation;
        this.nodeValues = initialNodeValue;
        this.progressLogger = progressLogger;
        this.executor = executor;

        var reducer = computation.reducer();

        this.messenger = reducer.isPresent()
            ? new ReducingMessenger(graph, config, reducer.get(), tracker)
            : config.isAsynchronous()
                ? new AsyncQueueMessenger(graph.nodeCount(), tracker)
                : new SyncQueueMessenger(graph.nodeCount(), tracker);

        this.computer = PregelComputer.<CONFIG>builder()
            .graph(graph)
            .computation(computation)
            .config(config)
            .nodeValues(nodeValues)
            .messenger(messenger)
            .voteBits(HugeAtomicBitSet.create(graph.nodeCount(), tracker))
            .executorService(config.useForkJoin()
                ? ParallelUtil.getFJPoolWithConcurrency(config.concurrency())
                : executor)
            .progressLogger(progressLogger)
            .build();
    }

    public PregelResult run() {
        boolean didConverge = false;

        computer.initComputation();

        int iteration = 0;
        for (; iteration < config.maxIterations(); iteration++) {
            logIterationStart(iteration);

            computer.initIteration(iteration);
            messenger.initIteration(iteration);

            computer.runIteration();

            didConverge = runMasterComputeStep(iteration) || computer.hasConverged();

            logIterationFinish(iteration, didConverge);

            if (didConverge) {
                break;
            }
        }

        return ImmutablePregelResult.builder()
            .nodeValues(nodeValues)
            .didConverge(didConverge)
            .ranIterations(iteration)
            .build();
    }

    public void release() {
        progressLogger.release();
        messenger.release();
    }

    private boolean runMasterComputeStep(int iteration) {
        progressLogger.startSubTask("Master Compute");
        var context = new MasterComputeContext<>(config, graph, iteration, nodeValues, executor);
        var didConverge = computation.masterCompute(context);
        progressLogger.finishSubTask("Master Compute");
        return didConverge;
    }

    private void logIterationStart(int iteration) {
        progressLogger
            .startSubTask(formatWithLocale("Iteration %d/%d", iteration + 1, config.maxIterations()));
    }

    private void logIterationFinish(int iteration, boolean didConverge) {
        var maxIterations = config.maxIterations();
        progressLogger.finishSubTask(formatWithLocale("Iteration %d/%d", iteration + 1, maxIterations));
        if (!didConverge && iteration < maxIterations - 1) {
            progressLogger.reset(graph.nodeCount());
        }
    }
}
