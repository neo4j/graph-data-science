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
package org.neo4j.gds.procedures.algorithms.pathfinding.stream;

import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFactoryFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.RandomWalkStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;

import java.util.stream.Stream;

class RandomWalkStreamResultTransformerBuilder implements ResultTransformerBuilder<TimedAlgorithmResult<Stream<long[]>>, Stream<RandomWalkStreamResult>> {
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final NodeLookup nodeLookup;
    private final boolean pathRequested;

    RandomWalkStreamResultTransformerBuilder(
        CloseableResourceRegistry closeableResourceRegistry,
        NodeLookup nodeLookup,
        boolean pathRequested
    ) {
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.nodeLookup = nodeLookup;
        this.pathRequested = pathRequested;
    }

    @Override
    public RandomWalkStreamResultTransformer build(GraphResources graphResources) {
        var graph = graphResources.graph();
        var graphStore = graphResources.graphStore();

        // this is us handling the case of generated graphs and such
        var pathFactoryFacade = PathFactoryFacade.create(
            pathRequested,
            nodeLookup,
            graphStore.capabilities().canWriteToLocalDatabase()
        );
        return new RandomWalkStreamResultTransformer(
            graph,
            closeableResourceRegistry,
            pathFactoryFacade
        );
    }
}
