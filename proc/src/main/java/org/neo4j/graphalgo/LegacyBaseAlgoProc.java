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
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.Pools;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.results.AbstractResultBuilder;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Deprecated
public abstract class LegacyBaseAlgoProc<A extends Algorithm<A, RESULT>, RESULT> extends BaseAlgoProc<A, RESULT, ProcedureConfiguration> {

    protected double getDefaultWeightProperty(ProcedureConfiguration config) {
        return ProcedureConstants.DEFAULT_VALUE_DEFAULT;
    }

    protected ProcedureConfiguration newConfig(String label, String relationship, Map<String, Object> config) {
        config.put("label", label);
        config.put("relationship", relationship);
        return newConfig(config);
    }

    protected ProcedureConfiguration newConfig(Map<String, Object> config) {
        return newConfig(Optional.empty(), CypherMapWrapper.create(config));
    }

    protected ProcedureConfiguration newConfig(Optional<String> graphName, Map<String, Object> config) {
        return newConfig(graphName, CypherMapWrapper.create(config));
    }

    protected abstract GraphLoader configureGraphLoader(GraphLoader loader, ProcedureConfiguration config);

    private Graph loadGraph(
        ProcedureConfiguration config,
        AllocationTracker tracker
    ) {
        return runWithExceptionLogging(
            "Loading failed",
            () -> {
                GraphLoader loader = getGraphLoader(config, tracker);

                return loader.load(config.getGraphImpl());
            }
        );
    }

    public GraphLoader getGraphLoader(ProcedureConfiguration config, AllocationTracker tracker) {
        GraphLoader loader = new GraphLoader(api, Pools.DEFAULT)
            .init(log, getUsername())
            .withAllocationTracker(tracker)
            .withTerminationFlag(TerminationFlag.wrap(transaction));

        loader = config.configureLoader(loader);
        loader = configureGraphLoader(loader, config);
        return loader;
    }

    protected final <R> Graph loadGraph(
        ProcedureConfiguration config,
        AllocationTracker tracker,
        AbstractResultBuilder<R> resultBuilder
    ) {
        try (ProgressTimer ignored = resultBuilder.timeLoad()) {
            Graph graph = loadGraph(config, tracker);
            resultBuilder
                .withNodeCount(graph.nodeCount())
                .withRelationshipCount(graph.relationshipCount());
            return graph;
        }
    }

    @Override
    public ProcedureConfiguration newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        ProcedureConfiguration configuration = (config != null)
            ? ProcedureConfiguration.create(config, getUsername())
            : ProcedureConfiguration.create(getUsername());

        String label = config.getString("label", "");
        if (label != null && !label.isEmpty()) {
            configuration = configuration.setNodeLabelOrQuery(label);
        }
        String relationship = config.getString("relationship", "");
        if (relationship != null && !relationship.isEmpty()) {
            configuration = configuration.setRelationshipTypeOrQuery(relationship);
        }

        configuration = configuration.setAlgoSpecificDefaultWeight(getDefaultWeightProperty(configuration));

        Set<String> returnItems = callContext.outputFields().collect(Collectors.toSet());
        return configuration
            .setComputeCommunityCount(OutputFieldParser.computeCommunityCount(returnItems))
            .setComputeHistogram(OutputFieldParser.computeHistogram(returnItems));
    }

    @Override
    protected MemoryTreeWithDimensions memoryEstimation(ProcedureConfiguration config) {
        GraphLoader loader = getGraphLoader(config, AllocationTracker.EMPTY);
        GraphFactory graphFactory = loader.build(config.getGraphImpl());
        GraphDimensions dimensions = graphFactory.dimensions();
        AlgorithmFactory<A, ProcedureConfiguration> algorithmFactory = algorithmFactory(config);
        MemoryEstimations.Builder estimationsBuilder = MemoryEstimations.builder("graph with procedure");

        MemoryEstimation graphMemoryEstimation = config.estimate(loader.toSetup(), graphFactory);
        estimationsBuilder.add(graphMemoryEstimation)
            .add(algorithmFactory.memoryEstimation(config));

        MemoryTree memoryTree = estimationsBuilder.build().estimate(dimensions, config.concurrency());

        return new MemoryTreeWithDimensions(memoryTree, dimensions);
    }
}
