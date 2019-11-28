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
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.newapi.BaseAlgoConfig;

import java.util.Optional;

public abstract class BaseAlgoProc<A extends Algorithm<A>, CONFIG extends BaseAlgoConfig> extends BaseProc<CONFIG> {

    protected abstract CONFIG newConfig(
        Optional<String> graphName,
        CypherMapWrapper config
    );

    protected final A newAlgorithm(
            final Graph graph,
            final CONFIG config,
            final AllocationTracker tracker) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        return algorithmFactory(config)
                .build(graph, config, tracker, log)
                .withProgressLogger(log)
                .withTerminationFlag(terminationFlag);
    }

    protected abstract GraphLoader configureGraphLoader(GraphLoader loader, CONFIG config);

    protected abstract AlgorithmFactory<A, CONFIG> algorithmFactory(CONFIG config);

    protected MemoryTreeWithDimensions memoryEstimation(final CONFIG config) {
        GraphLoader loader = newLoader(AllocationTracker.EMPTY, config);
        GraphFactory graphFactory = loader.build(config.getGraphImpl());
        GraphDimensions dimensions = graphFactory.dimensions();
        AlgorithmFactory<A, CONFIG> algorithmFactory = algorithmFactory(config);
        MemoryEstimations.Builder estimationsBuilder = MemoryEstimations.builder("graph with procedure");

        MemoryEstimation graphMemoryEstimation = config.estimate(loader.toSetup(), graphFactory);
        estimationsBuilder.add(graphMemoryEstimation)
            .add(algorithmFactory.memoryEstimation());

        MemoryTree memoryTree = estimationsBuilder.build().estimate(dimensions, config.concurrency());

        return new MemoryTreeWithDimensions(memoryTree, dimensions);
    }

    @Override
    protected GraphLoader newConfigureLoader(GraphLoader loader, CONFIG config) {
        boolean implicitLoading = true;
        if (implicitLoading) {
            return configureGraphLoader(loader, config);
        }
        return null;
    }
}
