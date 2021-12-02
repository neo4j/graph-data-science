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

import org.eclipse.collections.api.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.validation.Validator;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.logging.Log;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public class ProcedureExecutor<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig
> {

    private final ProcConfigParser<CONFIG, Pair<CONFIG, Optional<String>>> configParser;
    private final MemoryUsageValidator memoryUsageValidator;
    private final BiFunction<CONFIG, Optional<String>, GraphStoreLoader> graphStoreLoaderFn;
    private final Validator<CONFIG> validator;
    private final AlgorithmFactory<ALGO, CONFIG> algorithmFactory;
    private final KernelTransaction ktx;
    private final Log log;
    private final TaskRegistryFactory taskRegistryFactory;
    private final String procName;
    private final AllocationTracker allocationTracker;

    ProcedureExecutor(
        ProcConfigParser<CONFIG, Pair<CONFIG, Optional<String>>> configParser,
        MemoryUsageValidator memoryUsageValidator,
        BiFunction<CONFIG, Optional<String>, GraphStoreLoader> graphStoreLoaderFn,
        Validator<CONFIG> validator,
        AlgorithmFactory<ALGO, CONFIG> algorithmFactory,
        KernelTransaction ktx,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        String procName,
        AllocationTracker allocationTracker
    ) {
        this.configParser = configParser;
        this.memoryUsageValidator = memoryUsageValidator;
        this.graphStoreLoaderFn = graphStoreLoaderFn;
        this.validator = validator;
        this.algorithmFactory = algorithmFactory;
        this.ktx = ktx;
        this.log = log;
        this.taskRegistryFactory = taskRegistryFactory;
        this.procName = procName;
        this.allocationTracker = allocationTracker;
    }

    public GraphStore getOrCreateGraphStore(CONFIG config, GraphStoreLoader graphStoreLoader) {
        var graphCreateConfig = graphStoreLoader.graphCreateConfig();
        validator.validateConfigsBeforeLoad(graphCreateConfig, config);
        var graphStore = graphStoreLoader.graphStore();
        validator.validateConfigWithGraphStore(graphStore, graphCreateConfig, config);

        return graphStore;
    }

    protected AlgoBaseProc.ComputationResult<ALGO, ALGO_RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration,
        boolean releaseAlgorithm,
        boolean releaseTopology
    ) {
        ImmutableComputationResult.Builder<ALGO, ALGO_RESULT, CONFIG> builder = ImmutableComputationResult.builder();

        Pair<CONFIG, Optional<String>> input = configParser.processInput(graphNameOrConfig, configuration);
        var config = input.getOne();
        var maybeGraphName = input.getTwo();

        setAlgorithmMetaDataToTransaction(config);

        var graphStoreLoader = graphStoreLoaderFn.apply(config, maybeGraphName);
        var procedureMemoryEstimation = new ProcedureMemoryEstimation<>(graphStoreLoader, algorithmFactory);
        var memoryEstimationInBytes = memoryUsageValidator.tryValidateMemoryUsage(config, procedureMemoryEstimation::memoryEstimation);

        GraphStore graphStore;
        Graph graph;

        try (ProgressTimer timer = ProgressTimer.start(builder::createMillis)) {
            graphStore = getOrCreateGraphStore(config, graphStoreLoader);
            graph = createGraph(graphStore, config);
        }

        if (graph.isEmpty()) {
            return builder
                .isGraphEmpty(true)
                .graph(graph)
                .graphStore(graphStore)
                .config(config)
                .computeMillis(0)
                .result(null)
                .algorithm(null)
                .build();
        }

        ALGO algo = newAlgorithm(graph, graphStore, config, allocationTracker);

        algo.progressTracker.setEstimatedResourceFootprint(memoryEstimationInBytes, config.concurrency());

        ALGO_RESULT result = executeAlgorithm(releaseAlgorithm, releaseTopology, builder, graph, algo);

        log.info(procName + ": overall memory usage %s", allocationTracker.getUsageString());

        return builder
            .graph(graph)
            .graphStore(graphStore)
            .algorithm(algo)
            .result(result)
            .config(config)
            .build();
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
            .build(graph, graphStore, config, allocationTracker, log, taskRegistryFactory)
            .withTerminationFlag(terminationFlag);
    }

    private Graph createGraph(GraphStore graphStore, CONFIG config) {
        Optional<String> weightProperty = config instanceof RelationshipWeightConfig
            ? Optional.ofNullable(((RelationshipWeightConfig) config).relationshipWeightProperty())
            : Optional.empty();

        Collection<NodeLabel> nodeLabels = config.nodeLabelIdentifiers(graphStore);
        Collection<RelationshipType> relationshipTypes = config.internalRelationshipTypes(graphStore);

        return graphStore.getGraph(nodeLabels, relationshipTypes, weightProperty);
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
