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

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.GraphStoreAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;

import java.util.Map;
import java.util.function.Supplier;

public class ProcedureExecutor<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig,
    RESULT
> {

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

    public RESULT compute(
        String graphName,
        Map<String, Object> configuration
    ) {
        ProcPreconditions.check();

        ImmutableComputationResult.Builder<ALGO, ALGO_RESULT, CONFIG> builder = ImmutableComputationResult.builder();

        CONFIG config = executorSpec.configParser(algoSpec.newConfigFunction(), executionContext).processInput(configuration);

        executionContext.transactionApi().setAlgorithmMetaData(config);

        var graphCreation = executorSpec.graphCreationFactory(executionContext).create(config, graphName);

        var memoryEstimationInBytes = graphCreation.validateMemoryEstimation(algoSpec.algorithmFactory());

        GraphStore graphStore;
        Graph graph;

        try (ProgressTimer timer = ProgressTimer.start(builder::preProcessingMillis)) {
            var graphProjectConfig = graphCreation.graphProjectConfig();
            var validator = executorSpec.validator(algoSpec.validationConfig());
            validator.validateConfigsBeforeLoad(graphProjectConfig, config);
            graphStore = graphCreation.graphStore();
            validator.validateConfigWithGraphStore(graphStore, graphProjectConfig, config);
            graph = graphCreation.createGraph(graphStore);
        }

        if (graph.isEmpty()) {
            var emptyComputationResult = builder
                .isGraphEmpty(true)
                .graph(graph)
                .graphStore(graphStore)
                .config(config)
                .computeMillis(0)
                .result(null)
                .algorithm(null)
                .build();
            return algoSpec.computationResultConsumer().consume(emptyComputationResult, executionContext);
        }

        ALGO algo = newAlgorithm(graph, graphStore, config);

        algo.getProgressTracker().setEstimatedResourceFootprint(memoryEstimationInBytes, config.concurrency());

        ALGO_RESULT result = executeAlgorithm(builder, algo);

        var computationResult = builder
            .graph(graph)
            .graphStore(graphStore)
            .algorithm(algo)
            .result(result)
            .config(config)
            .build();

        return algoSpec.computationResultConsumer().consume(computationResult, executionContext);
    }

    private ALGO_RESULT executeAlgorithm(
        ImmutableComputationResult.Builder<ALGO, ALGO_RESULT, CONFIG> builder,
        ALGO algo
    ) {
        return runWithExceptionLogging(
            "Computation failed",
            () -> {
                try (ProgressTimer ignored = ProgressTimer.start(builder::computeMillis)) {
                    return algo.compute();
                } catch (Throwable e) {
                    algo.getProgressTracker().endSubTaskWithFailure();
                    throw e;
                } finally {
                    if (algoSpec.releaseProgressTask()) {
                        algo.getProgressTracker().release();
                    }
                }
            }
        );
    }

    private ALGO newAlgorithm(
        Graph graph,
        GraphStore graphStore,
        CONFIG config
    ) {
        TerminationFlag terminationFlag = executionContext.transactionApi().terminationFlag();
        ALGO algorithm = algoSpec.algorithmFactory()
            .accept(new AlgorithmFactory.Visitor<>() {
                @Override
                public ALGO graph(GraphAlgorithmFactory<ALGO, CONFIG> graphAlgorithmFactory) {
                    return graphAlgorithmFactory.build(
                        graph,
                        config,
                        executionContext.log(),
                        executionContext.taskRegistryFactory(),
                        executionContext.userLogRegistryFactory()
                    );
                }

                @Override
                public ALGO graphStore(GraphStoreAlgorithmFactory<ALGO, CONFIG> graphStoreAlgorithmFactory) {
                    return graphStoreAlgorithmFactory.build(
                        graphStore,
                        config,
                        executionContext.log(),
                        executionContext.taskRegistryFactory(),
                        executionContext.userLogRegistryFactory()
                    );
                }
            });
        algorithm.setTerminationFlag(terminationFlag);

        return algorithm;
    }

    private <R> R runWithExceptionLogging(String message, Supplier<R> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            executionContext.log().warn(message, e);
            throw e;
        }
    }
}
