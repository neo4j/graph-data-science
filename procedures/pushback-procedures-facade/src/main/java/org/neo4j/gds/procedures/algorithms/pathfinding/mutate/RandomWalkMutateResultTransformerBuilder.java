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

import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.pathfinding.RandomWalkCountingNodeVisitsMutateStep;
import org.neo4j.gds.procedures.algorithms.pathfinding.RandomWalkMutateResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;
import org.neo4j.gds.traversal.RandomWalkMutateConfig;

import java.util.stream.Stream;

class RandomWalkMutateResultTransformerBuilder implements ResultTransformerBuilder<TimedAlgorithmResult<HugeAtomicLongArray>, Stream<RandomWalkMutateResult>> {

    private final MutateNodePropertyService mutateNodePropertyService;
    private final RandomWalkMutateConfig configuration;

    RandomWalkMutateResultTransformerBuilder(
        MutateNodePropertyService mutateNodePropertyService,
        RandomWalkMutateConfig configuration
    ) {
        this.mutateNodePropertyService = mutateNodePropertyService;
        this.configuration = configuration;
    }

    @Override
    public RandomWalkMutateResultTransformer build(
        GraphResources graphResources
    ) {
        var mutateStep = new RandomWalkCountingNodeVisitsMutateStep(
            configuration.nodeLabels(),
            configuration.mutateProperty(),
            mutateNodePropertyService
        );
        return new RandomWalkMutateResultTransformer(
            mutateStep,
            graphResources.graph(),
            graphResources.graphStore(), 
            configuration.toMap()
        );
    }
}
