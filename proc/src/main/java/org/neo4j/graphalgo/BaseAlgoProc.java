/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.loading.GraphCatalog;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.newapi.BaseAlgoConfig;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.helpers.collection.Pair;

import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

public abstract class BaseAlgoProc<A extends Algorithm<A, RESULT>, RESULT, CONFIG extends BaseAlgoConfig> extends BaseProc {

    public String algoName() {
        return this.getClass().getSimpleName();
    }

    protected abstract CONFIG newConfig(
        Optional<String> graphName,
        CypherMapWrapper config
    );

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

    protected MemoryTreeWithDimensions memoryEstimation(final CONFIG config) {
        MemoryEstimations.Builder estimationBuilder = MemoryEstimations.builder("Memory Estimation");
        GraphDimensions dimensions;

        if (config.implicitCreateConfig().isPresent()) {
            GraphLoader loader = newLoader(AllocationTracker.EMPTY, config.implicitCreateConfig().get());
            GraphFactory graphFactory = loader.build(config.getGraphImpl());
            dimensions = graphFactory.dimensions();
            estimationBuilder.add("graph", graphFactory.memoryEstimation());
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

            GraphLoader loader = newLoader(AllocationTracker.EMPTY, graphCreateConfig);
            GraphFactory graphFactory = loader.build(config.getGraphImpl());
            dimensions = graphFactory.dimensions();
        }

        estimationBuilder.add("algorithm", algorithmFactory(config).memoryEstimation());

        MemoryTree memoryTree = estimationBuilder.build().estimate(dimensions, config.concurrency());
        return new MemoryTreeWithDimensions(memoryTree, dimensions);
    }

    Pair<CONFIG, Optional<String>> processInput(Object graphNameOrConfig, Map<String, Object> configuration) {
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
        Optional<String> graphName = configAndName.other();

        if (graphName.isPresent()) {
            return GraphCatalog.getUnion(getUsername(), graphName.get()).orElseThrow(
                () -> new NoSuchElementException(String.format("Cannot find graph with name %s", graphName.get()))
            );
        } else if (config.implicitCreateConfig().isPresent()) {
            GraphCreateConfig createConfig = config.implicitCreateConfig().get();

            GraphLoader loader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, getUsername())
                .withGraphCreateConfig(createConfig)
                .withAllocationTracker(AllocationTracker.EMPTY)
                .withTerminationFlag(TerminationFlag.wrap(transaction));

            return loader.load(createConfig.getGraphImpl());
        } else {
            throw new IllegalStateException("There must be either a graph name or an implicit create config");
        }
    }

    ComputationResult<A, RESULT, CONFIG> compute(Object graphNameOrConfig, Map<String, Object> configuration) {
        ImmutableComputationResult.Builder<A, RESULT, CONFIG> builder = ImmutableComputationResult.builder();

        Pair<CONFIG, Optional<String>> input = processInput(graphNameOrConfig, configuration);
        CONFIG config = input.first();

        Graph graph;
        try (ProgressTimer timer = ProgressTimer.start(builder::createMillis)) {
            graph = createGraph(input);
        }

        AllocationTracker tracker = AllocationTracker.create();

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

    @ValueClass
    interface ComputationResult<A extends Algorithm<A, RESULT>, RESULT, CONFIG extends BaseAlgoConfig> {
        long createMillis();

        long computeMillis();

        A algorithm();

        RESULT result();

        Graph graph();

        AllocationTracker tracker();

        CONFIG config();
    }
}
