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
package org.neo4j.gds.beta.pregel;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.beta.pregel.context.MasterComputeContext;
import org.neo4j.gds.core.concurrency.ExecutorServiceUtil;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.progress.tasks.Tasks;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.utils.StringJoining;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public final class Pregel<CONFIG extends PregelConfig> {

    private final CONFIG config;

    private final BasePregelComputation<CONFIG> computation;

    private final Graph graph;

    private final NodeValue nodeValues;

    private final Messenger<?> messenger;

    private final PregelComputer<CONFIG> computer;

    private final ProgressTracker progressTracker;
    private final TerminationFlag terminationFlag;

    private final ExecutorService executor;

    public static <CONFIG extends PregelConfig> Pregel<CONFIG> create(
        Graph graph,
        CONFIG config,
        BasePregelComputation<CONFIG> computation,
        ExecutorService executor,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        // This prevents users from disabling concurrency
        // validation in custom PregelConfig implementations.
        // Creating a copy of the user config triggers the
        // concurrency validations.
        PregelConfigImpl.Builder.from(config).build();

        if (computation instanceof BidirectionalPregelComputation && !graph.characteristics().isInverseIndexed()) {
            throw new UnsupportedOperationException(String.format(
                Locale.US,
                "The Pregel algorithm %s requires inverse indexes for all configured relationships %s",
                computation.getClass().getSimpleName(),
                StringJoining.join(config.relationshipTypes())
            ));
        }

        return new Pregel<>(
            graph,
            config,
            computation,
            NodeValue.of(computation.schema(config), graph.nodeCount(), config.concurrency()),
            executor,
            progressTracker,
            terminationFlag
        );
    }

    public static MemoryEstimation memoryEstimation(
        Map<String, ValueType> propertiesMap,
        boolean isQueueBased,
        boolean isAsync
    ) {
        return memoryEstimation(propertiesMap, isQueueBased, isAsync, false);
    }

    public static MemoryEstimation memoryEstimation(
        Map<String, ValueType> propertiesMap,
        boolean isQueueBased,
        boolean isAsync,
        boolean isTrackingSender
    ) {
        var estimationBuilder = MemoryEstimations.builder(Pregel.class)
            .perNode("vote bits", HugeAtomicBitSet::memoryEstimation)
            .perThread("compute steps", MemoryEstimations.builder(PartitionedComputeStep.class).build())
            .add("node value", NodeValue.memoryEstimation(propertiesMap));

        if (isQueueBased) {
            if (isAsync) {
                estimationBuilder.add("message queues", AsyncQueueMessenger.memoryEstimation());
            } else {
                estimationBuilder.add("message queues", SyncQueueMessenger.memoryEstimation());
            }
        } else {
            estimationBuilder.add("message arrays", ReducingMessenger.memoryEstimation(isTrackingSender));
        }

        return estimationBuilder.build();
    }

    public static <CONFIG extends PregelConfig> Task progressTask(long nodeCount, CONFIG config, MemoryRange memoryEstimationInBytes, String taskName) {
        return Tasks.iterativeDynamic(
            taskName,
            config.concurrency(), () -> List.of(
                Tasks.leaf("Compute iteration", config.concurrency(), nodeCount),
                Tasks.leaf("Master compute iteration", config.concurrency(), nodeCount)
            ),
            memoryEstimationInBytes,
            config.maxIterations()
        );
    }

    public static <CONFIG extends PregelConfig> Task progressTask(Graph graph, CONFIG config, MemoryRange memoryEstimationInBytes) {
        var configName = config.getClass().getSimpleName();
        var taskName = configName.replaceAll(
            "(Mutate|Stream|Write|Stats)*Config",
            ""
        );
        return progressTask(graph.nodeCount(), config, memoryEstimationInBytes, taskName);
    }

    private Pregel(
        final Graph graph,
        final CONFIG config,
        final BasePregelComputation<CONFIG> computation,
        final NodeValue initialNodeValue,
        final ExecutorService executor,
        final ProgressTracker progressTracker,
        final TerminationFlag terminationFlag
    ) {
        this.graph = graph;
        this.config = config;
        this.computation = computation;
        this.nodeValues = initialNodeValue;
        this.executor = executor;
        this.progressTracker = progressTracker;
        this.terminationFlag = terminationFlag;

        var reducer = computation.reducer();

        this.messenger = reducer.isPresent()
            ? ReducingMessenger.create(graph, config, reducer.get())
            : config.isAsynchronous()
                ? new AsyncQueueMessenger(graph.nodeCount())
                : new SyncQueueMessenger(graph.nodeCount());

        this.computer = PregelComputer.<CONFIG>builder()
            .graph(graph)
            .computation(computation)
            .config(config)
            .nodeValues(nodeValues)
            .messenger(messenger)
            .voteBits(HugeAtomicBitSet.create(graph.nodeCount()))
            .executorService(config.useForkJoin()
                ? ExecutorServiceUtil.createForkJoinPool(config.concurrency())
                : executor)
            .progressTracker(progressTracker)
            .build();
    }

    public PregelResult run() {
        boolean didConverge = false;

        computer.initComputation();

        try {
            progressTracker.beginSubTask();

            int iteration = 0;
            for (; iteration < config.maxIterations(); iteration++) {
                terminationFlag.assertRunning();
                progressTracker.beginSubTask();

                computer.initIteration(iteration);
                messenger.initIteration(iteration);
                computer.runIteration();

                progressTracker.endSubTask();

                progressTracker.beginSubTask();

                didConverge = runMasterComputeStep(iteration) || computer.hasConverged();

                progressTracker.endSubTask();

                if (didConverge) {
                    break;
                }
            }
            progressTracker.endSubTask();
            return new PregelResult(nodeValues, iteration, didConverge);
        } catch (Exception e) {
            progressTracker.endSubTaskWithFailure();
            throw e;
        } finally {
            computer.release();
        }
    }

    public void release() {
        progressTracker.release();
        messenger.release();
    }

    private boolean runMasterComputeStep(int iteration) {
        var context = new MasterComputeContext<>(config, graph, iteration, nodeValues, executor, progressTracker);

        return computation.masterCompute(context);
    }
}
