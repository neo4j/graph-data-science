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
package org.neo4j.gds.procedures.algorithms.pathfinding.mutate;

import org.neo4j.gds.applications.algorithms.machinery.MutateRelationshipService;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.pathfinding.BellmanFordMutateStep;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordMutateConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.BellmanFordMutateResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;

import java.util.stream.Stream;

class BellmanFordMutateResultTransformerBuilder implements ResultTransformerBuilder<TimedAlgorithmResult<BellmanFordResult>, Stream<BellmanFordMutateResult>> {

    private final MutateRelationshipService mutateRelationshipService;
    private final AllShortestPathsBellmanFordMutateConfig configuration;

    BellmanFordMutateResultTransformerBuilder(
        MutateRelationshipService mutateRelationshipService,
        AllShortestPathsBellmanFordMutateConfig configuration
    ) {
        this.mutateRelationshipService = mutateRelationshipService;
        this.configuration = configuration;
    }

    @Override
    public BellmanFordMutateResultTransformer build(
        GraphResources graphResources
    ) {
        var mutateStep = new BellmanFordMutateStep(
            configuration.mutateRelationshipType(),
            configuration.mutateNegativeCycles(),
            mutateRelationshipService
        );
        return new BellmanFordMutateResultTransformer(
            mutateStep,
            graphResources.graph(),
            graphResources.graphStore(),
            configuration.toMap()
        );
    }
}
