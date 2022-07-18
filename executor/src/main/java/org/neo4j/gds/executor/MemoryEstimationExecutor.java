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
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.api.ImmutableGraphLoaderContext;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.transaction.TransactionContext;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryEstimationExecutor<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig
    > {

    private final AlgorithmSpec<ALGO, ALGO_RESULT, CONFIG, ?, ?> algoSpec;
    private final ExecutorSpec<ALGO, ALGO_RESULT, CONFIG> executorSpec;
    private final ExecutionContext executionContext;

    public MemoryEstimationExecutor(
        AlgorithmSpec<ALGO, ALGO_RESULT, CONFIG, ?, ?> algoSpec,
        ExecutorSpec<ALGO, ALGO_RESULT, CONFIG> executorSpec,
        ExecutionContext executionContext
    ) {
        this.algoSpec = algoSpec;
        this.executorSpec = executorSpec;
        this.executionContext = executionContext;
    }

    public MemoryEstimationExecutor(
        AlgorithmSpec<ALGO, ALGO_RESULT, CONFIG, ?, ?> algoSpec,
        ExecutionContext executionContext
    ) {
        this(algoSpec, algoSpec.createDefaultExecutorSpec(), executionContext);
    }


    public Stream<MemoryEstimateResult> computeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        var configParser = executorSpec.configParser(algoSpec.newConfigFunction(), executionContext);
        CONFIG algoConfig = configParser.processInput(algoConfiguration);

        GraphDimensions graphDims;
        Optional<MemoryEstimation> maybeGraphEstimation;

        if (graphNameOrConfiguration instanceof Map) {
            // if the api is null, we are probably using EstimationCli
            var graphLoaderContext = (executionContext.databaseService() == null)
                ? GraphLoaderContext.NULL_CONTEXT
                : ImmutableGraphLoaderContext
                    .builder()
                    .graphDatabaseService(executionContext.databaseService())
                    .log(executionContext.log())
                    .taskRegistryFactory(executionContext.taskRegistryFactory())
                    .userLogRegistryFactory(executionContext.userLogRegistryFactory())
                    .terminationFlag(TerminationFlag.wrap(executionContext.transaction()))
                    .transactionContext(TransactionContext.of(
                        executionContext.databaseService(),
                        executionContext.procedureTransaction()
                    )).build();

            var memoryEstimationGraphConfigParser = new MemoryEstimationGraphConfigParser(executionContext.username());
            var graphProjectConfig = memoryEstimationGraphConfigParser.processInput(graphNameOrConfiguration);
            var graphStoreCreator = graphProjectConfig.isFictitiousLoading()
                ? new FictitiousGraphStoreLoader(graphProjectConfig)
                : new GraphStoreFromDatabaseLoader(graphProjectConfig, executionContext.username(), graphLoaderContext);

            graphDims = graphStoreCreator.graphDimensions();
            maybeGraphEstimation = Optional.of(graphStoreCreator.estimateMemoryUsageAfterLoading());
        } else if (graphNameOrConfiguration instanceof String) {
            graphDims = new GraphStoreFromCatalogLoader(
                (String) graphNameOrConfiguration,
                algoConfig,
                executionContext.username(),
                executionContext.databaseId(),
                executionContext.isGdsAdmin()
            ).graphDimensions();
            maybeGraphEstimation = Optional.empty();
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Expected `graphNameOrConfiguration` to be of type String or Map, but got",
                graphNameOrConfiguration.getClass().getSimpleName()
            ));
        }
        MemoryTreeWithDimensions memoryTreeWithDimensions = procedureMemoryEstimation(
            graphDims,
            maybeGraphEstimation,
            algoSpec.algorithmFactory(),
            algoConfig
        );

        return Stream.of(new MemoryEstimateResult(memoryTreeWithDimensions));
    }

    protected MemoryTreeWithDimensions procedureMemoryEstimation(
        GraphDimensions dimensions,
        Optional<MemoryEstimation> maybeEstimation,
        AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory,
        CONFIG config
    ) {
        MemoryEstimations.Builder estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        GraphDimensions extendedDimension = algorithmFactory.estimatedGraphDimensionTransformer(dimensions, config);

        maybeEstimation.ifPresent(graphMemoryEstimation -> estimationBuilder.add("graph", graphMemoryEstimation));
        estimationBuilder.add("algorithm", algorithmFactory.memoryEstimation(config));

        MemoryTree memoryTree = estimationBuilder.build().estimate(extendedDimension, config.concurrency());

        return new MemoryTreeWithDimensions(memoryTree, dimensions);
    }
}
