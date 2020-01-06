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

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.ModernGraphLoader;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.results.MemoryEstimateResult;
import org.neo4j.graphalgo.newapi.AlgoBaseConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.newapi.SeedConfig;
import org.neo4j.graphalgo.newapi.WeightConfig;
import org.neo4j.graphalgo.newapi.WriteConfig;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.helpers.collection.Pair;

import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.core.ProcedureConstants.IS_EXPLICIT_CYPHER_GRAPH;

public abstract class AlgoBaseProc<A extends Algorithm<A, RESULT>, RESULT, CONFIG extends AlgoBaseConfig> extends BaseProc {

    protected static final String ESTIMATE_DESCRIPTION = "Returns an estimation of the memory consumption for that procedure.";
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
        if (!graphName.isPresent()) {
            // we should do implicit loading
            maybeImplicitCreate = Optional.of(GraphCreateConfig.implicitCreate(getUsername(), config));
        }
        return newConfig(getUsername(), graphName, maybeImplicitCreate, config);
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
        GraphDimensions dimensions;

        if (config.implicitCreateConfig().isPresent()) {
            GraphCreateConfig createConfig = config.implicitCreateConfig().get();
            ModernGraphLoader loader = newLoader(createConfig, AllocationTracker.EMPTY);
            GraphFactory graphFactory = loader.build(config.getGraphImpl());
            dimensions = graphFactory.dimensions();

            if (createConfig.nodeCount() >= 0 || createConfig.relationshipCount() >= 0) {
                dimensions.nodeCount(createConfig.nodeCount());
                dimensions.maxRelCount(createConfig.relationshipCount());
            }

            estimationBuilder.add("graph", graphFactory.memoryEstimation(graphFactory.setup(), dimensions));
        } else {
            String graphName = config.graphName().get();

            // TODO get the dimensions from the graph itself.
            GraphCreateConfig graphCreateConfig = GraphCatalog
                .getLoadedGraphs(getUsername())
                .keySet()
                .stream()
                .filter(graph -> graph.graphName().equals(graphName))
                .findFirst()
                .get();

            ModernGraphLoader loader = newLoader(graphCreateConfig, AllocationTracker.EMPTY);
            GraphFactory graphFactory = loader.build(config.getGraphImpl());
            dimensions = graphFactory.dimensions();
        }

        estimationBuilder.add("algorithm", algorithmFactory(config).memoryEstimation(config));

        MemoryTree memoryTree = estimationBuilder.build().estimate(dimensions, config.concurrency());
        return new MemoryTreeWithDimensions(memoryTree, dimensions);
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

        return Pair.of(config, graphName);
    }

    protected Graph createGraph(Pair<CONFIG, Optional<String>> configAndName) {
        CONFIG config = configAndName.first();
        Optional<String> maybeGraphName = configAndName.other();

        Map.Entry<GraphCreateConfig, Graph> catalogEntry;

        Optional<String> weightProperty = config instanceof WeightConfig ?
            Optional.ofNullable(((WeightConfig) config).weightProperty()) : Optional.empty();

        if (maybeGraphName.isPresent()) {
            catalogEntry = GraphCatalog
                .filterLoadedGraphs(getUsername(), maybeGraphName.get(), config.relationshipTypes(), weightProperty)
                .entrySet()
                .stream()
                .filter(e -> e.getKey().graphName().equals(maybeGraphName.get()))
                .findFirst().orElseThrow(
                    () -> new NoSuchElementException(String.format("Cannot find graph with name %s", maybeGraphName.get()))
                );

            validateConfig(catalogEntry.getKey(), config);

            return catalogEntry.getValue();
        } else if (config.implicitCreateConfig().isPresent()) {
            GraphCreateConfig createConfig = config.implicitCreateConfig().get();
            validateConfig(createConfig, config);
            ModernGraphLoader loader = newLoader(createConfig, AllocationTracker.EMPTY);

            return loader.load(createConfig.getGraphImpl());
        } else {
            throw new IllegalStateException("There must be either a graph name or an implicit create config");
        }
    }

    private void validateConfig(GraphCreateConfig graphCreateConfig, CONFIG config) {
        if (graphCreateConfig.nodeProjection().labelProjection().orElse("not_cypher").equals(IS_EXPLICIT_CYPHER_GRAPH)) {
            return;
        }
        if (config instanceof SeedConfig) {
            Set<String> nodeProperties = graphCreateConfig.nodeProjection().allProperties();
            String seedProperty = ((SeedConfig) config).seedProperty();
            if (seedProperty != null && !nodeProperties.contains(seedProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Seed property `%s` not found in graph with node properties: %s",
                    seedProperty,
                    nodeProperties
                ));
            }
        }
        if (config instanceof WeightConfig) {
            Set<String> properties = new HashSet<>();
            properties.addAll(graphCreateConfig.relationshipProjection().allProperties());
            properties.addAll(graphCreateConfig.nodeProjection().allProperties());

            String weightProperty = ((WeightConfig) config).weightProperty();
            if (weightProperty != null && !properties.contains(weightProperty)) {
                throw new IllegalArgumentException(String.format(
                    "Weight property `%s` not found in graph with relationship properties: %s",
                    weightProperty,
                    properties
                ));
            }
        }
    }

    protected ComputationResult<A, RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        ImmutableComputationResult.Builder<A, RESULT, CONFIG> builder = ImmutableComputationResult.builder();
        AllocationTracker tracker = AllocationTracker.create();

        Pair<CONFIG, Optional<String>> input = processInput(graphNameOrConfig, configuration);
        CONFIG config = input.first();

        Graph graph;

        try (ProgressTimer timer = ProgressTimer.start(builder::createMillis)) {
            graph = createGraph(input);
        }

        if (graph.isEmpty()) {
            return builder
                .isGraphEmpty(true)
                .graph(graph)
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

        algo.release();
        graph.releaseTopology();

        return builder
            .graph(graph)
            .tracker(AllocationTracker.EMPTY)
            .algorithm(algo)
            .result(result)
            .config(config)
            .build();
    }

    protected PropertyTranslator<RESULT> nodePropertyTranslator(
        ComputationResult<A, RESULT, CONFIG> computationResult
    ) {
        throw new UnsupportedOperationException("Write procedures needs to implement org.neo4j.graphalgo.BaseAlgoProc.nodePropertyTranslator");
    }

    protected void writeNodeProperties(
        AbstractResultBuilder<?, ?> writeBuilder,
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

    protected Stream<MemoryEstimateResult> computeEstimate(Object graphNameOrConfig, Map<String, Object> configuration) {
        Pair<CONFIG, Optional<String>> configAndGraphName = processInput(
            graphNameOrConfig,
            configuration
        );

        MemoryTreeWithDimensions memoryTreeWithDimensions = memoryEstimation(configAndGraphName.first());
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

        AllocationTracker tracker();

        CONFIG config();

        @Value.Default
        default boolean isGraphEmpty() {
            return false;
        }
    }
}
