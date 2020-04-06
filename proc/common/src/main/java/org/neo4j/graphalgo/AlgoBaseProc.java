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
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.config.MutatePropertyConfig;
import org.neo4j.graphalgo.config.MutateRelationshipConfig;
import org.neo4j.graphalgo.config.NodeWeightConfig;
import org.neo4j.graphalgo.config.RandomGraphGeneratorConfig;
import org.neo4j.graphalgo.config.RelationshipWeightConfig;
import org.neo4j.graphalgo.config.SeedConfig;
import org.neo4j.graphalgo.config.WritePropertyConfig;
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
import org.neo4j.graphalgo.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.AbstractProjections.PROJECT_ALL;

public abstract class AlgoBaseProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig> extends BaseProc {

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
    protected final ALGO newAlgorithm(
        final Graph graph,
        final CONFIG config,
        final AllocationTracker tracker
    ) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        return algorithmFactory(config)
            .build(graph, config, tracker, log)
            .withTerminationFlag(terminationFlag);
    }

    protected abstract AlgorithmFactory<ALGO, CONFIG> algorithmFactory(CONFIG config);

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
            String graphName = config.graphName().orElseThrow(IllegalStateException::new);

            GraphCreateConfig graphCreateConfig = GraphStoreCatalog.get(getUsername(), graphName).config();

            // TODO get the dimensions from the graph itself.
            if (graphCreateConfig instanceof RandomGraphGeneratorConfig) {
                estimateDimensions = ImmutableGraphDimensions.builder()
                    .nodeCount(graphCreateConfig.nodeCount())
                    .maxRelCount(((RandomGraphGeneratorConfig) graphCreateConfig).averageDegree() * graphCreateConfig.nodeCount())
                    .build();
            } else {
                GraphCreateConfig filteredConfig = filterGraphCreateConfig(config, graphCreateConfig);

                GraphLoader loader = newLoader(filteredConfig, AllocationTracker.EMPTY);
                GraphStoreFactory graphStoreFactory = loader.build(config.getGraphImpl());
                estimateDimensions = graphStoreFactory.dimensions();
            }
        }

        estimationBuilder.add("algorithm", algorithmFactory(config).memoryEstimation(config));

        MemoryTree memoryTree = estimationBuilder.build().estimate(estimateDimensions, config.concurrency());
        return new MemoryTreeWithDimensions(memoryTree, estimateDimensions);
    }

    private GraphCreateConfig filterGraphCreateConfig(CONFIG config, GraphCreateConfig graphCreateConfig) {
        NodeProjections nodeProjections = graphCreateConfig.nodeProjections();
        List<ElementIdentifier> nodeLabels = config.nodeLabels().stream().map(ElementIdentifier::of).collect(Collectors.toList());
        if (nodeLabels.contains(PROJECT_ALL)) {
            return graphCreateConfig;
        } else {
            NodeProjections.Builder builder = NodeProjections.builder();
            nodeProjections
                .projections()
                .entrySet()
                .stream()
                .filter(projection -> nodeLabels.contains(projection.getKey()))
                .forEach(entry -> builder.putProjection(entry.getKey(), entry.getValue()));
            NodeProjections filteredNodeProjections = builder.build();
            return GraphCreateFromStoreConfig.of(
                config.username(),
                config.graphName().get(),
                filteredNodeProjections,
                graphCreateConfig.relationshipProjections(),
                CypherMapWrapper.create(config.toMap())
            );
        }
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

    private Graph createGraph(GraphStore graphStore, CONFIG config) {
        Optional<String> weightProperty = config instanceof RelationshipWeightConfig
            ? Optional.ofNullable(((RelationshipWeightConfig) config).relationshipWeightProperty())
            : Optional.empty();

        List<ElementIdentifier> nodeLabels = config.nodeLabelIdentifiers();
        List<String> relationshipTypes = config.relationshipTypes();

        return graphStore.getGraph(nodeLabels, relationshipTypes, weightProperty, config.concurrency());
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

        validate(graphCandidate, config);
        return graphCandidate.graphStore();
    }

    private void validate(GraphStoreWithConfig graphStoreWithConfig, CONFIG config) {
        GraphStore graphStore = graphStoreWithConfig.graphStore();
        GraphCreateConfig graphCreateConfig = graphStoreWithConfig.config();

        if (graphCreateConfig instanceof GraphCreateFromCypherConfig) {
            return;
        }

        Collection<ElementIdentifier> filterLabels = config.nodeLabelIdentifiers().contains(PROJECT_ALL)
            ? graphStore.nodeLabels()
            : config.nodeLabelIdentifiers();
        if (config instanceof SeedConfig) {
            String seedProperty = ((SeedConfig) config).seedProperty();
            if (seedProperty != null && !graphStore.hasNodeProperty(filterLabels, seedProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Seed property `%s` not found in graph with node properties: %s",
                    seedProperty,
                    graphStore.nodePropertyKeys().values()
                ));
            }
        }
        if (config instanceof NodeWeightConfig) {
            String weightProperty = ((NodeWeightConfig) config).nodeWeightProperty();
            if (weightProperty != null && !graphStore.hasNodeProperty(filterLabels, weightProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Node weight property `%s` not found in graph with node properties: %s",
                    weightProperty,
                    graphStore.nodePropertyKeys().values()
                ));
            }
        }
        if (config instanceof RelationshipWeightConfig) {
            Set<String> properties = graphStore.relationshipPropertyKeys();

            String weightProperty = ((RelationshipWeightConfig) config).relationshipWeightProperty();
            if (weightProperty != null && !properties.contains(weightProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Relationship weight property `%s` not found in graph with relationship properties: %s",
                    weightProperty,
                    properties
                ));
            }
        }

        if (config instanceof MutatePropertyConfig) {
            MutatePropertyConfig mutateConfig = (MutatePropertyConfig) config;
            String mutateProperty = mutateConfig.mutateProperty();

            if (mutateProperty != null && graphStore.hasNodeProperty(filterLabels, mutateProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Node property `%s` already exists in the in-memory graph.",
                    mutateProperty
                ));
            }
        }

        if (config instanceof MutateRelationshipConfig) {
            String mutateRelationshipType = ((MutateRelationshipConfig) config).mutateRelationshipType();
            if (mutateRelationshipType != null && graphStore.hasRelationshipType(mutateRelationshipType)) {
                throw new IllegalArgumentException(String.format(
                    "Relationship type `%s` already exists in the in-memory graph.",
                    mutateRelationshipType
                ));
            }
        }

        validateConfigs(graphCreateConfig, config);
    }

    protected void validateConfigs(GraphCreateConfig graphCreateConfig, CONFIG config) { }

    protected ComputationResult<ALGO, ALGO_RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        return compute(graphNameOrConfig, configuration, true, true);
    }

    protected ComputationResult<ALGO, ALGO_RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration,
        boolean releaseAlgorithm,
        boolean releaseTopology
    ) {
        ImmutableComputationResult.Builder<ALGO, ALGO_RESULT, CONFIG> builder = ImmutableComputationResult.builder();
        AllocationTracker tracker = AllocationTracker.create();

        Pair<CONFIG, Optional<String>> input = processInput(graphNameOrConfig, configuration);
        CONFIG config = input.getOne();
        validateMemoryUsageIfImplemented(config);

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

        ALGO algo = newAlgorithm(graph, config, tracker);

        ALGO_RESULT result = runWithExceptionLogging(
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

    protected PropertyTranslator<ALGO_RESULT> nodePropertyTranslator(
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        throw new UnsupportedOperationException(
            "Write procedures needs to implement org.neo4j.graphalgo.BaseAlgoProc.nodePropertyTranslator");
    }

    protected void writeNodeProperties(
        AbstractResultBuilder<?> writeBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    ) {
        PropertyTranslator<ALGO_RESULT> resultPropertyTranslator = nodePropertyTranslator(computationResult);

        CONFIG config = computationResult.config();
        if (!(config instanceof WritePropertyConfig)) {
            throw new IllegalArgumentException(String.format(
                "Can only write results if the config implements %s.",
                WritePropertyConfig.class
            ));
        }

        WritePropertyConfig writePropertyConfig = (WritePropertyConfig) config;
        try (ProgressTimer ignored = ProgressTimer.start(writeBuilder::withWriteMillis)) {
            log.debug("Writing results");

            Graph graph = computationResult.graph();
            TerminationFlag terminationFlag = computationResult.algorithm().getTerminationFlag();
            NodePropertyExporter exporter = NodePropertyExporter.of(api, graph, terminationFlag)
                .withLog(log)
                .parallel(Pools.DEFAULT, writePropertyConfig.writeConcurrency())
                .build();

            exporter.write(
                writePropertyConfig.writeProperty(),
                computationResult.result(),
                resultPropertyTranslator
            );
            writeBuilder.withNodePropertiesWritten(exporter.propertiesWritten());
        }
    }

    protected void validateMemoryUsageIfImplemented(CONFIG config) {
        MemoryTreeWithDimensions memoryTreeWithDimensions = null;
        try {
            memoryTreeWithDimensions = memoryEstimation(config);
        } catch (MemoryEstimationNotImplementedException ignored) {
        }
        if (memoryTreeWithDimensions != null) {
            validateMemoryUsage(memoryTreeWithDimensions);
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
