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
package org.neo4j.gds.executor;

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;
import org.neo4j.gds.metrics.telemetry.TelemetryLoggerImpl;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.graphdb.TransactionTerminatedHelper;
import org.neo4j.kernel.api.exceptions.Status;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public class ProcedureExecutor<ALGO extends Algorithm<ALGO_RESULT>, ALGO_RESULT, CONFIG extends AlgoBaseConfig, RESULT> {
    private final AlgorithmSpec<ALGO, ALGO_RESULT, CONFIG, RESULT, ?> algoSpec;
    private final ExecutorSpec<ALGO, ALGO_RESULT, CONFIG> executorSpec;
    private final ExecutionContext executionContext;

    public ProcedureExecutor(
        AlgorithmSpec<ALGO, ALGO_RESULT, CONFIG, RESULT, ?> algoSpec,
        ExecutorSpec<ALGO, ALGO_RESULT, CONFIG> executorSpec,
        ExecutionContext executionContext
    ) {
        this.algoSpec = algoSpec;
        this.executorSpec = executorSpec;
        this.executionContext = executionContext;
    }

    public ProcedureExecutor(
        AlgorithmSpec<ALGO, ALGO_RESULT, CONFIG, RESULT, ?> algoSpec,
        ExecutionContext executionContext
    ) {
        this(algoSpec, algoSpec.createDefaultExecutorSpec(), executionContext);
    }

    public RESULT compute(String graphName, Map<String, Object> configuration) {
        CONFIG config = executorSpec.configParser(algoSpec.newConfigFunction(), executionContext).processInput(configuration);

        var graphCreation = executorSpec.graphCreationFactory(executionContext).create(config, graphName);

        var memoryEstimationInBytes = graphCreation.validateMemoryEstimation(algoSpec.algorithmFactory(executionContext));

        GraphStore graphStore;
        Graph graph;
        ResultStore resultStore;

        ComputationResultBuilder<ALGO, ALGO_RESULT, CONFIG> builder = ComputationResult.builder();

        try (var ignored = ProgressTimer.start(builder::preProcessingMillis)) {
            var graphProjectConfig = graphCreation.graphProjectConfig();
            var validator = executorSpec.validator(algoSpec.validationConfig(executionContext));
            validator.validateConfigsBeforeLoad(graphProjectConfig, config);
            graphStore = graphCreation.graphStore();
            resultStore = graphCreation.resultStore();
            validator.validateConfigWithGraphStore(graphStore, graphProjectConfig, config);
            graph = graphCreation.createGraph(graphStore);
        }

        if (graph.isEmpty()) {
            var emptyComputationResult = builder
                .graph(graph)
                .graphStore(graphStore)
                .resultStore(resultStore)
                .config(config)
                .computeMillis(0)
                .result(Optional.empty())
                .algorithm(null)
                .build();
            return algoSpec.computationResultConsumer().consume(emptyComputationResult, executionContext);
        }

        Pair<ALGO, ProgressTracker> algorithmAndProgressTracker = newAlgorithm(graph, config, memoryEstimationInBytes);

        ALGO_RESULT result = executeAlgorithm(
            builder,
            algorithmAndProgressTracker.getLeft(),
            executionContext.metrics().algorithmMetrics(),
            graphStore,
            config,
            algorithmAndProgressTracker.getRight()
        );

        var computationResult = builder
            .graph(graph)
            .graphStore(graphStore)
            .resultStore(resultStore)
            .algorithm(algorithmAndProgressTracker.getLeft())
            .result(result)
            .config(config)
            .build();

        return algoSpec.computationResultConsumer().consume(computationResult, executionContext);
    }

    private ALGO_RESULT executeAlgorithm(
        ComputationResultBuilder<ALGO, ALGO_RESULT, CONFIG> builder,
        ALGO algo,
        AlgorithmMetricsService algorithmMetricsService,
        GraphStore graphStore,
        CONFIG config,
        ProgressTracker progressTracker
    ) {
        return runWithExceptionLogging(
            () -> {
                var telemetryLogger = new TelemetryLoggerImpl(executionContext.log());
                var algorithmMetric = algorithmMetricsService.create(
                    // we don't want to use `spec.name()` because it's different for the different procedure modes;
                    // we want to capture the algorithm name as defined by the algorithm factory `taskName()`
                    algoSpec.algorithmFactory(executionContext).taskName()
                );

                try (var timer = ProgressTimer.start(builder::computeMillis); algorithmMetric) {
                    algorithmMetric.start();

                    var result = algo.compute();

                    timer.stop();

                    var graphIdentifier = System.identityHashCode(graphStore);
                    telemetryLogger.logAlgorithm(graphIdentifier, algoSpec.name(), config, timer.getDuration());

                    return result;
                } catch (Exception e) {
                    progressTracker.endSubTaskWithFailure();
                    algorithmMetric.failed(e);

                    throw e;
                } finally {
                    if (algoSpec.releaseProgressTask()) {
                        progressTracker.release();
                    }
                }
            }
        );
    }

    private Pair<ALGO, ProgressTracker> newAlgorithm(
        Graph graph,
        CONFIG config,
        MemoryRange memoryEstimationInBytes
    ) {
        var terminationFlag = TerminationFlag.wrap(
            executionContext.terminationMonitor(),
            () -> TransactionTerminatedHelper.transactionTerminated(Status.Transaction.Terminated)
        );

        return algoSpec.algorithmFactory(executionContext)
            .accept(graphAlgorithmFactory -> graphAlgorithmFactory.build(
                graph,
                config,
                executionContext.log(),
                executionContext.taskRegistryFactory(),
                terminationFlag,
                memoryEstimationInBytes
            ));
    }

    private <R> R runWithExceptionLogging(Supplier<R> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            executionContext.log().warn("Computation failed", e);
            throw e;
        }
    }
}
