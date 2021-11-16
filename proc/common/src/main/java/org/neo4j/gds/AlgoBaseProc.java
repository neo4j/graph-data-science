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
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.validation.ValidationConfiguration;
import org.neo4j.gds.validation.Validator;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public abstract class AlgoBaseProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig> extends BaseProc {

    protected static final String STATS_DESCRIPTION = "Executes the algorithm and returns result statistics without writing the result to Neo4j.";
    protected String procName() {
        return this.getClass().getSimpleName();
    }

    public ConfigParser<CONFIG> configParser() {
        return new ConfigParser<>(username()) {
            @Override
            protected CONFIG newConfig(
                String username,
                Optional<String> graphName,
                Optional<GraphCreateConfig> maybeImplicitCreate,
                CypherMapWrapper config
            ) {
                return AlgoBaseProc.this.newConfig(username, graphName, maybeImplicitCreate, config);
            }
        };
    }

    private void setAlgorithmMetaDataToTransaction(CONFIG algoConfig) {
        if (transaction == null) {
            return;
        }
        var metaData = transaction.getMetaData();
        if (metaData instanceof AlgorithmMetaData) {
            ((AlgorithmMetaData) metaData).set(algoConfig);
        }
    }

    protected abstract CONFIG newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    );

    protected abstract AlgorithmFactory<ALGO, CONFIG> algorithmFactory();

    protected ComputationResult<ALGO, ALGO_RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        return compute(graphNameOrConfig, configuration, true, true);
    }

    protected ComputationResult<ALGO, ALGO_RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration,
        boolean releaseAlgorithm,
        boolean releaseTopology
    ) {
        ImmutableComputationResult.Builder<ALGO, ALGO_RESULT, CONFIG> builder = ImmutableComputationResult.builder();
        var allocationTracker = allocationTracker();

        Pair<CONFIG, Optional<String>> input = configParser().processInput(graphNameOrConfig, configuration);
        CONFIG config = input.getOne();

        setAlgorithmMetaDataToTransaction(config);

        var memoryEstimationInBytes = tryValidateMemoryUsage(config, this::memoryEstimation);

        GraphStore graphStore;
        Graph graph;

        try (ProgressTimer timer = ProgressTimer.start(builder::createMillis)) {
            graphStore = getOrCreateGraphStore(input);
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

        ALGO algo = newAlgorithm(graph, config, allocationTracker);

        algo.progressTracker.setEstimatedResourceFootprint(memoryEstimationInBytes, config.concurrency());

        ALGO_RESULT result = runWithExceptionLogging(
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

        log.info(procName() + ": overall memory usage %s", allocationTracker.getUsageString());

        return builder
            .graph(graph)
            .graphStore(graphStore)
            .algorithm(algo)
            .result(result)
            .config(config)
            .build();
    }

    /**
     * Returns a single node property that has been produced by the procedure.
     */
    protected NodeProperties nodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        throw new UnsupportedOperationException("Procedure must implement org.neo4j.gds.AlgoBaseProc.nodeProperty");
    }

    protected Stream<MemoryEstimateResult> computeEstimate(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        Pair<CONFIG, Optional<String>> configAndGraphName = configParser().processInput(
            graphNameOrConfig,
            configuration
        );

        MemoryTreeWithDimensions memoryTreeWithDimensions = memoryEstimation(configAndGraphName.getOne());
        return Stream.of(
            new MemoryEstimateResult(memoryTreeWithDimensions)
        );
    }

    MemoryTreeWithDimensions memoryEstimation(CONFIG config) {
        MemoryEstimations.Builder estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        var graphStoreLoader = graphStoreLoader(config, config.graphName());

        GraphDimensions estimateDimensions = graphStoreLoader.graphDimensions();
        graphStoreLoader.memoryEstimation().map(graphEstimation -> estimationBuilder.add("graph", graphEstimation));

        estimationBuilder.add("algorithm", algorithmFactory().memoryEstimation(config));

        MemoryTree memoryTree = estimationBuilder.build().estimate(estimateDimensions, config.concurrency());
        return new MemoryTreeWithDimensions(memoryTree, estimateDimensions);
    }

    private ALGO newAlgorithm(
        final Graph graph,
        final CONFIG config,
        final AllocationTracker allocationTracker
    ) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        return algorithmFactory()
            .build(graph, config, allocationTracker, log, taskRegistryFactory)
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

    public ValidationConfiguration<CONFIG> getValidationConfig() {
        return ValidationConfiguration.empty();
    }

    protected GraphStore getOrCreateGraphStore(Pair<CONFIG, Optional<String>> configAndName) {
        CONFIG config = configAndName.getOne();
        Optional<String> maybeGraphName = configAndName.getTwo();
        Validator<CONFIG> validator = new Validator<>(getValidationConfig());
        var graphStoreLoader = graphStoreLoader(config, maybeGraphName);

        var graphCreateConfig = graphStoreLoader.graphCreateConfig();
        validator.validateConfigsBeforeLoad(graphCreateConfig, config);
        var graphStore = graphStoreLoader.graphStore();
        validator.validateConfigWithGraphStore(graphStore, graphCreateConfig, config);

        return graphStore;
    }

    private GraphStoreLoader graphStoreLoader(CONFIG config, Optional<String> maybeGraphName) {
        return GraphStoreLoader.of(
            config,
            maybeGraphName,
            this::databaseId,
            this::username,
            this::graphLoaderContext,
            isGdsAdmin()
        );
    }

    @ValueClass
    public interface ComputationResult<A extends Algorithm<A, RESULT>, RESULT, CONFIG extends AlgoBaseConfig> {
        long createMillis();

        long computeMillis();

        @Nullable
        A algorithm();

        @Nullable
        RESULT result();

        Graph graph();

        GraphStore graphStore();

        CONFIG config();

        @Value.Default
        default boolean isGraphEmpty() {
            return false;
        }
    }
}
