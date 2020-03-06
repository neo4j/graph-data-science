/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphStoreFactory;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.config.GraphCreateFromCypherConfig;
import org.neo4j.graphalgo.config.NodeWeightConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphalgo.config.SeedConfig;
import org.neo4j.graphalgo.config.WriteConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ImmutableGraphDimensions;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.GraphStore;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.loading.GraphStoreWithConfig;
import org.neo4j.graphalgo.core.loading.ImmutableGraphStoreWithConfig;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

public abstract class AlgoBaseProc<A extends Algorithm<A, RESULT>, RESULT, CONFIG extends AlgoBaseConfig> extends BaseProc {

    protected static final String STATS_DESCRIPTION = "Executes the algorithm and returns result statistics without writing the result to Neo4j.";

    public String algoName() {
        return this.getClass().getSimpleName();
    }

    protected abstract CONFIG newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    );

    public final CONFIG newConfig(Optional<String> graphName, CypherMapWrapper config) {
        Optional<GraphCreateConfig> maybeImplicitCreate = Optional.empty();
        Collection<String> allowedKeys = new HashSet<>();
        if (!graphName.isPresent()) {
            // we should do implicit loading
            GraphCreateConfig createConfig = GraphCreateConfig.createImplicit(getUsername(), config);
            maybeImplicitCreate = Optional.of(createConfig);
            allowedKeys.addAll(createConfig.configKeys());
            config = config.withoutAny(allowedKeys);
        }
        CONFIG algoConfig = newConfig(getUsername(), graphName, maybeImplicitCreate, config);
        allowedKeys.addAll(algoConfig.configKeys());
        validateConfig(config, allowedKeys);
        return algoConfig;
    }

    // TODO make AlgorithmFactory have a constructor that accepts CONFIG
    protected final A newAlgorithm(
        final Graph graph,
        final CONFIG config,
        final AllocationTracker tracker
    ) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        return algorithmFactory(config)
            .build(graph, config, tracker, log)
            .withProgressLogger(log)
            .withTerminationFlag(terminationFlag);
    }

    protected abstract AlgorithmFactory<A, CONFIG> algorithmFactory(CONFIG config);

    protected MemoryTreeWithDimensions memoryEstimation(CONFIG config) {
        MemoryEstimations.Builder estimationBuilder = MemoryEstimations.builder("Memory Estimation");
        GraphDimensions estimateDimensions;

        if (config.implicitCreateConfig().isPresent()) {
            GraphCreateConfig createConfig = config.implicitCreateConfig().get();
            GraphLoader loader = newLoader(createConfig, AllocationTracker.EMPTY);
            GraphStoreFactory graphStoreFactory = loader.build(config.getGraphImpl());
            estimateDimensions = graphStoreFactory.dimensions();

            if (createConfig.nodeCount() >= 0 || createConfig.relationshipCount() >= 0) {
                estimateDimensions = ImmutableGraphDimensions.builder()
                    .from(estimateDimensions)
                    .nodeCount(createConfig.nodeCount())
                    .highestNeoId(createConfig.nodeCount())
                    .maxRelCount(createConfig.relationshipCount())
                    .build();
            }

            estimationBuilder.add("graph", graphStoreFactory.memoryEstimation(estimateDimensions));
        } else {
            String graphName = config.graphName().get();

            // TODO get the dimensions from the graph itself.
            GraphCreateConfig graphCreateConfig = GraphStoreCatalog
                .getLoadedGraphs(getUsername())
                .keySet()
                .stream()
                .filter(graph -> graph.graphName().equals(graphName))
                .findFirst()
                .get();

            GraphLoader loader = newLoader(graphCreateConfig, AllocationTracker.EMPTY);
            GraphStoreFactory graphStoreFactory = loader.build(config.getGraphImpl());
            estimateDimensions = graphStoreFactory.dimensions();
        }

        estimationBuilder.add("algorithm", algorithmFactory(config).memoryEstimation(config));

        MemoryTree memoryTree = estimationBuilder.build().estimate(estimateDimensions, config.concurrency());
        return new MemoryTreeWithDimensions(memoryTree, estimateDimensions);
    }

    protected Pair<CONFIG, Optional<String>> processInput(Object graphNameOrConfig, Map<String, Object> configuration) {
        CONFIG config;
        Optional<String> graphName = Optional.empty();

        if (graphNameOrConfig instanceof String) {
            graphName = Optional.of((String) graphNameOrConfig);
            CypherMapWrapper algoConfig = CypherMapWrapper.create(configuration);
            config = newConfig(graphName, algoConfig);

            //TODO: assert that algoConfig is empty or fail
        } else if (graphNameOrConfig instanceof Map) {
            if (!configuration.isEmpty()) {
                throw new IllegalArgumentException(
                    "The second parameter can only used when a graph name is given as first parameter");
            }

            Map<String, Object> implicitConfig = (Map<String, Object>) graphNameOrConfig;
            CypherMapWrapper implicitAndAlgoConfig = CypherMapWrapper.create(implicitConfig);

            config = newConfig(Optional.empty(), implicitAndAlgoConfig);

            //TODO: assert that implicitAndAlgoConfig is empty or fail
        } else {
            throw new IllegalArgumentException(
                "The first parameter must be a graph name or a configuration map, but was: " + graphNameOrConfig
            );
        }

        return Tuples.pair(config, graphName);
    }

    protected Graph createGraph(Pair<CONFIG, Optional<String>> configAndName) {
        return createGraph(getOrCreateGraphStore(configAndName), configAndName.getOne());
    }

    private GraphStore getOrCreateGraphStore(Pair<CONFIG, Optional<String>> configAndName) {
        CONFIG config = configAndName.getOne();
        Optional<String> maybeGraphName = configAndName.getTwo();

        GraphStoreWithConfig graphCandidate;

        if (maybeGraphName.isPresent()) {
            graphCandidate = GraphStoreCatalog.get(getUsername(), maybeGraphName.get());
        } else if (config.implicitCreateConfig().isPresent()) {
            GraphCreateConfig createConfig = config.implicitCreateConfig().get();
            GraphLoader loader = newLoader(createConfig, AllocationTracker.EMPTY);
            GraphStore graphStore = loader.build(createConfig.getGraphImpl()).build().graphStore();

            graphCandidate = ImmutableGraphStoreWithConfig.of(graphStore, createConfig);
        } else {
            throw new IllegalStateException("There must be either a graph name or an implicit create config");
        }

        validateConfig(graphCandidate.config(), config);
        return graphCandidate.graphStore();
    }

    private Graph createGraph(GraphStore graphStore, CONFIG config) {
        Optional<String> weightProperty = config instanceof RelationshipWeightConfig
            ? Optional.ofNullable(((RelationshipWeightConfig) config).relationshipWeightProperty())
            : Optional.empty();

        List<String> relationshipTypes = config.relationshipTypes();

        return graphStore.getGraph(relationshipTypes, weightProperty);
    }

    private void validateConfig(GraphCreateConfig graphCreateConfig, CONFIG config) {
        if (graphCreateConfig instanceof GraphCreateFromCypherConfig) {
            return;
        }
        if (config instanceof SeedConfig) {
            Set<String> nodeProperties = graphCreateConfig.nodeProjections().allProperties();
            String seedProperty = ((SeedConfig) config).seedProperty();
            if (seedProperty != null && !nodeProperties.contains(seedProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Seed property `%s` not found in graph with node properties: %s",
                    seedProperty,
                    nodeProperties
                ));
            }
        }
        if (config instanceof NodeWeightConfig) {
            Set<String> properties = new HashSet<>(graphCreateConfig.nodeProjections().allProperties());

            String weightProperty = ((NodeWeightConfig) config).nodeWeightProperty();
            if (weightProperty != null && !properties.contains(weightProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Node weight property `%s` not found in graph with node properties: %s",
                    weightProperty,
                    properties
                ));
            }
        }
        if (config instanceof RelationshipWeightConfig) {
            Set<String> properties = new HashSet<>(graphCreateConfig.relationshipProjections().allProperties());

            String weightProperty = ((RelationshipWeightConfig) config).relationshipWeightProperty();
            if (weightProperty != null && !properties.contains(weightProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Relationship weight property `%s` not found in graph with relationship properties: %s",
                    weightProperty,
                    properties
                ));
            }
        }

        validateGraphCreateConfig(graphCreateConfig, config);
    }

    protected void validateGraphCreateConfig(GraphCreateConfig graphCreateConfig, CONFIG config) { }

    protected ComputationResult<A, RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        return compute(graphNameOrConfig, configuration, true, true);
    }

    protected ComputationResult<A, RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration,
        boolean releaseAlgorithm,
        boolean releaseTopology
    ) {
        ImmutableComputationResult.Builder<A, RESULT, CONFIG> builder = ImmutableComputationResult.builder();
        AllocationTracker tracker = AllocationTracker.create();

        Pair<CONFIG, Optional<String>> input = processInput(graphNameOrConfig, configuration);
        CONFIG config = input.getOne();

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
                .tracker(tracker)
                .computeMillis(0)
                .result(null)
                .algorithm(null)
                .build();
        }

        A algo = newAlgorithm(graph, config, tracker);

        RESULT result = runWithExceptionLogging(
            "Computation failed",
            () -> {
                try (ProgressTimer ignored = ProgressTimer.start(builder::computeMillis)) {
                    return algo.compute();
                }
            }
        );

        log.info(algoName() + ": overall memory usage %s", tracker.getUsageString());

        if (releaseAlgorithm) {
            algo.release();
        }
        if (releaseTopology) {
            graph.releaseTopology();
        }

        return builder
            .graph(graph)
            .graphStore(graphStore)
            .tracker(AllocationTracker.EMPTY)
            .algorithm(algo)
            .result(result)
            .config(config)
            .build();
    }

    protected PropertyTranslator<RESULT> nodePropertyTranslator(
        ComputationResult<A, RESULT, CONFIG> computationResult
    ) {
        throw new UnsupportedOperationException(
            "Write procedures needs to implement org.neo4j.graphalgo.BaseAlgoProc.nodePropertyTranslator");
    }

    protected interface WriteOrMutate<A extends Algorithm<A, RESULT>, RESULT, CONFIG extends AlgoBaseConfig> {
        void apply(AbstractResultBuilder<?> writeBuilder, ComputationResult<A, RESULT, CONFIG> computationResult);
    }

    protected void writeNodeProperties(
        AbstractResultBuilder<?> writeBuilder,
        ComputationResult<A, RESULT, CONFIG> computationResult
    ) {
        PropertyTranslator<RESULT> resultPropertyTranslator = nodePropertyTranslator(computationResult);

        CONFIG config = computationResult.config();
        if (!(config instanceof WriteConfig)) {
            throw new IllegalArgumentException(String.format(
                "Can only write results if the config implements %s.",
                WriteConfig.class
            ));
        }

        WriteConfig writeConfig = (WriteConfig) config;
        try (ProgressTimer ignored = ProgressTimer.start(writeBuilder::withWriteMillis)) {
            log.debug("Writing results");

            Graph graph = computationResult.graph();
            TerminationFlag terminationFlag = computationResult.algorithm().getTerminationFlag();
            NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, terminationFlag)
                .withLog(log)
                .parallel(Pools.DEFAULT, writeConfig.writeConcurrency())
                .build();

            exporter.write(
                writeConfig.writeProperty(),
                computationResult.result(),
                resultPropertyTranslator
            );
            writeBuilder.withNodePropertiesWritten(exporter.propertiesWritten());
        }
    }

    protected void mutateNodeProperties(
        AbstractResultBuilder<?> writeBuilder,
        ComputationResult<A, RESULT, CONFIG> computationResult
    ) {
        PropertyTranslator<RESULT> resultPropertyTranslator = nodePropertyTranslator(computationResult);

        CONFIG config = computationResult.config();
        if (!(config instanceof WriteConfig)) {
            throw new IllegalArgumentException(String.format(
                "Can only write results if the config implements %s.",
                WriteConfig.class
            ));
        }
        WriteConfig writeConfig = (WriteConfig) config;
        RESULT result = computationResult.result();
        try (ProgressTimer ignored = ProgressTimer.start(writeBuilder::withWriteMillis)) {
            log.debug("Updating graph store");
            GraphStore graphStore = computationResult.graphStore();
            graphStore.addNodeProperty(
                writeConfig.writeProperty(),
                nodeId -> resultPropertyTranslator.toDouble(result, nodeId)
            );
            writeBuilder.withNodePropertiesWritten(computationResult.graph().nodeCount());
        }
    }

    protected Stream<MemoryEstimateResult> computeEstimate(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        Pair<CONFIG, Optional<String>> configAndGraphName = processInput(
            graphNameOrConfig,
            configuration
        );

        MemoryTreeWithDimensions memoryTreeWithDimensions = memoryEstimation(configAndGraphName.getOne());
        return Stream.of(
            new MemoryEstimateResult(memoryTreeWithDimensions)
        );
    }

    protected boolean shouldWrite(CONFIG config) {
        return config instanceof WriteConfig;
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

        AllocationTracker tracker();

        CONFIG config();

        @Value.Default
        default boolean isGraphEmpty() {
            return false;
        }
    }
}
