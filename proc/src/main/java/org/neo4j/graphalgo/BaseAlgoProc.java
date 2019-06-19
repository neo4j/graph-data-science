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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.GraphFactory;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.results.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.impl.results.AbstractResultBuilder;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

public abstract class BaseAlgoProc<A extends ConfiguredAlgorithm<A, Conf>, Conf> {

    @Context
    public GraphDatabaseAPI api;

    @Context
    public Log log;

    @Context
    public KernelTransaction transaction;

    abstract GraphLoader configureLoader(
            GraphLoader loader,
            ProcedureConfiguration config);

    abstract Conf algoConfig(
            ProcedureConfiguration config,
            final Optional<Graph> graph);

    abstract A algorithm(
            ProcedureConfiguration config,
            Conf algoConfig,
            AllocationTracker tracker,
            Optional<Graph> graph
    );

    final ProcedureConfiguration newConfig(
            final String label,
            final String relationship,
            final Map<String, Object> config) {

        ProcedureConfiguration configuration = ProcedureConfiguration.create(config);
        if (label != null && !label.isEmpty()) {
            configuration.overrideNodeLabelOrQuery(label);
        }
        if (relationship != null && !relationship.isEmpty()) {
            configuration.overrideRelationshipTypeOrQuery(relationship);
        }
        return configuration;
    }

    final GraphLoader newLoader(
            final ProcedureConfiguration config,
            final AllocationTracker tracker) {
        String label = config.getNodeLabelOrQuery();
        String relationship = config.getRelationshipOrQuery();
        GraphLoader loader = new GraphLoader(api, Pools.DEFAULT)
                .init(log, label, relationship, config)
                .withAllocationTracker(tracker);
        return configureLoader(loader, config);
    }

    final GraphLoader newLoader(
            final String label,
            final String relationship,
            final Map<String, Object> config,
            final AllocationTracker tracker) {
        return newLoader(newConfig(label, relationship, config), tracker);
    }

    private Graph loadGraph(
            final ProcedureConfiguration config,
            final AllocationTracker tracker) {
        return newLoader(config, tracker).load(config.getGraphImpl());
    }

    final <R> Graph loadGraph(
            final ProcedureConfiguration config,
            final AllocationTracker tracker,
            final AbstractResultBuilder<R> result) {
        try (ProgressTimer ignored = result.timeLoad()) {
            return loadGraph(config, tracker);
        }
    }

    final <R> Graph loadGraph(
            final ProcedureConfiguration config,
            final AllocationTracker tracker,
            final AbstractCommunityResultBuilder<R> result) {
        try (ProgressTimer ignored = result.timeLoad()) {
            return loadGraph(config, tracker);
        }
    }

    final Graph loadGraph(
            final ProcedureConfiguration config,
            final AllocationTracker tracker,
            final Supplier<ProgressTimer> timer) {
        try (ProgressTimer ignored = timer.get()) {
            return loadGraph(config, tracker);
        }
    }

    final MemoryTreeWithDimensions memoryEstimation(final ProcedureConfiguration config) {
        GraphLoader loader = newLoader(config, AllocationTracker.EMPTY);
        GraphFactory graphFactory = loader.build(config.getGraphImpl());
        AlgoWithConfig<A, Conf> algoWithConfig = newAlgorithm(config, AllocationTracker.EMPTY, Optional.empty());
        A algo = algoWithConfig.algo();
        MemoryEstimation estimation = MemoryEstimations.builder("graph with algo")
                .add(algo.memoryEstimation(algoWithConfig.conf()))
                .add(graphFactory.memoryEstimation())
                .build();
        MemoryTree memoryTree = estimation.estimate(graphFactory.dimensions(), config.getConcurrency());
        return new MemoryTreeWithDimensions(memoryTree, graphFactory.dimensions());
    }

    final AlgoWithConfig<A, Conf> newAlgorithm(
            final ProcedureConfiguration config,
            final AllocationTracker tracker,
            final Optional<Graph> graph) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        Conf conf = algoConfig(config, graph);
        A algorithm = algorithm(config, conf, tracker, graph);
        A algo = algorithm
                .withLog(log)
                .withTerminationFlag(terminationFlag);
        return AlgoWithConfig.of(algo, conf);
    }
}
