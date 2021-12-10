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
package org.neo4j.gds;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.pipeline.ComputationResultConsumer;
import org.neo4j.gds.pipeline.GraphCreationFactory;
import org.neo4j.gds.validation.Validator;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

import java.util.Map;
import java.util.function.Supplier;

public class ProcedureExecutor<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig,
    RESULT
> {

    private final ProcConfigParser<CONFIG> configParser;
    private final Validator<CONFIG> validator;
    private final AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory;
    private final KernelTransaction ktx;
    private final Log log;
    private final TaskRegistryFactory taskRegistryFactory;
    private final String procName;
    private final AllocationTracker allocationTracker;
    private final ComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, RESULT> computationResultConsumer;
    private final GraphCreationFactory<ALGO, ALGO_RESULT, CONFIG> graphCreationFactory;

    public ProcedureExecutor(
        ProcConfigParser<CONFIG> configParser,
        Validator<CONFIG> validator,
        AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory,
        KernelTransaction ktx,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        String procName,
        AllocationTracker allocationTracker,
        ComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, RESULT> computationResultConsumer,
        GraphCreationFactory<ALGO, ALGO_RESULT, CONFIG> graphCreationFactory
    ) {
        this.configParser = configParser;
        this.validator = validator;
        this.algorithmFactory = algorithmFactory;
        this.ktx = ktx;
        this.log = log;
        this.taskRegistryFactory = taskRegistryFactory;
        this.procName = procName;
        this.allocationTracker = allocationTracker;
        this.computationResultConsumer = computationResultConsumer;
        this.graphCreationFactory = graphCreationFactory;
    }

    public RESULT compute(
        String graphName,
        Map<String, Object> configuration,
        boolean releaseAlgorithm,
        boolean releaseTopology
    ) {
        ImmutableComputationResult.Builder<ALGO, ALGO_RESULT, CONFIG> builder = ImmutableComputationResult.builder();

        CONFIG config = configParser.processInput(configuration);

        setAlgorithmMetaDataToTransaction(config);

        var graphCreation = graphCreationFactory.create(config, graphName);

        var memoryEstimationInBytes = graphCreation.validateMemoryEstimation(algorithmFactory);

        GraphStore graphStore;
        Graph graph;

        try (ProgressTimer timer = ProgressTimer.start(builder::createMillis)) {
            var graphCreateConfig = graphCreation.graphCreateConfig();
            validator.validateConfigsBeforeLoad(graphCreateConfig, config);
            graphStore = graphCreation.graphStore();
            validator.validateConfigWithGraphStore(graphStore, graphCreateConfig, config);
            graph = graphCreation.createGraph(graphStore);
        }

        if (graph.isEmpty()) {
            return computationResultConsumer.consume(builder
                .isGraphEmpty(true)
                .graph(graph)
                .graphStore(graphStore)
                .config(config)
                .computeMillis(0)
                .result(null)
                .algorithm(null)
                .build());
        }

        ALGO algo = newAlgorithm(graph, graphStore, config, allocationTracker);

        algo.progressTracker.setEstimatedResourceFootprint(memoryEstimationInBytes, config.concurrency());

        ALGO_RESULT result = executeAlgorithm(releaseAlgorithm, releaseTopology, builder, graph, algo);

        log.info(procName + ": overall memory usage %s", allocationTracker.getUsageString());

        return computationResultConsumer.consume(builder
            .graph(graph)
            .graphStore(graphStore)
            .algorithm(algo)
            .result(result)
            .config(config)
            .build());
    }

    private ALGO_RESULT executeAlgorithm(
        boolean releaseAlgorithm,
        boolean releaseTopology,
        ImmutableComputationResult.Builder<ALGO, ALGO_RESULT, CONFIG> builder,
        Graph graph,
        ALGO algo
    ) {
        return runWithExceptionLogging(
            "Computation failed",
            () -> {
                try (ProgressTimer ignored = ProgressTimer.start(builder::computeMillis)) {
                    return algo.compute();
                } catch (Throwable e) {
                    algo.progressTracker.fail();
                    throw e;
                } finally {
                    if (releaseAlgorithm) {
                        algo.progressTracker.release();
                        algo.release();
                    }
                    if (releaseTopology) {
                        graph.releaseTopology();
                    }
                }
            }
        );
    }

    private ALGO newAlgorithm(
        Graph graph,
        GraphStore graphStore,
        CONFIG config,
        AllocationTracker allocationTracker
    ) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(ktx);
        return algorithmFactory
            .accept(new AlgorithmFactory.Visitor<>() {
                @Override
                public ALGO graph(GraphAlgorithmFactory<ALGO, CONFIG> graphAlgorithmFactory) {
                    return graphAlgorithmFactory.build(graph, config, allocationTracker, log, taskRegistryFactory);
                }

                @Override
                public ALGO graphStore(GraphStoreAlgorithmFactory<ALGO, CONFIG> graphStoreAlgorithmFactory) {
                    return graphStoreAlgorithmFactory.build(graphStore, config, allocationTracker, log, taskRegistryFactory);
                }
            })
            .withTerminationFlag(terminationFlag);
    }

    private void setAlgorithmMetaDataToTransaction(CONFIG algoConfig) {
        if (ktx == null) {
            return;
        }
        var metaData = ktx.getMetaData();
        if (metaData instanceof AlgorithmMetaData) {
            ((AlgorithmMetaData) metaData).set(algoConfig);
        }
    }

    private <R> R runWithExceptionLogging(String message, Supplier<R> supplier) {
        try {
            return supplier.get();
        } catch (Exception e) {
            log.warn(message, e);
            throw e;
        }
    }
}
