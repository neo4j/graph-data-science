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
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.GraphLoader;
import org.neo4j.graphalgo.core.ProcedureConfiguration;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryTree;
import org.neo4j.graphalgo.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

public abstract class BaseAlgoProc<A extends Algorithm<A>> extends BaseProc {

    protected final A newAlgorithm(
            final Graph graph,
            final ProcedureConfiguration config,
            final AllocationTracker tracker) {
        TerminationFlag terminationFlag = TerminationFlag.wrap(transaction);
        return algorithmFactory(config)
                .build(graph, config, tracker, log)
                .withProgressLogger(log)
                .withTerminationFlag(terminationFlag);
    }

    protected abstract GraphLoader configureGraphLoader(GraphLoader loader, ProcedureConfiguration config);

    protected abstract AlgorithmFactory<A> algorithmFactory(ProcedureConfiguration config);

    protected MemoryTreeWithDimensions memoryEstimation(final ProcedureConfiguration config) {
        GraphLoader loader = newLoader(config, AllocationTracker.EMPTY);
        GraphFactory graphFactory = loader.build(config.getGraphImpl());
        GraphDimensions dimensions = graphFactory.dimensions();
        AlgorithmFactory<A> algorithmFactory = algorithmFactory(config);
        MemoryEstimations.Builder estimationsBuilder = MemoryEstimations.builder("graph with procedure");

        estimationsBuilder.add(getGraphMemoryEstimation(config, loader, graphFactory))
            .add(algorithmFactory.memoryEstimation());

        MemoryTree memoryTree = estimationsBuilder.build().estimate(dimensions, config.getConcurrency());

        return new MemoryTreeWithDimensions(memoryTree, dimensions);
    }

    protected double getDefaultWeightProperty(ProcedureConfiguration config) {
        return ProcedureConstants.DEFAULT_VALUE_DEFAULT;
    }

    @Override
    protected GraphLoader configureLoader(GraphLoader loader, ProcedureConfiguration config) {
        if (config.hasWeightProperty()) {
            loader.withRelationshipProperties(PropertyMapping.of(
                    config.getWeightProperty(),
                    config.getWeightPropertyDefaultValue(getDefaultWeightProperty(config))
            ));
        }
        return configureGraphLoader(loader, config);
    }
}
