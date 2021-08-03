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
package org.neo4j.gds.paths;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.graphalgo.MutateProc;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.DefaultValue;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.config.AlgoBaseConfig;
import org.neo4j.graphalgo.config.MutateRelationshipConfig;
import org.neo4j.graphalgo.core.Aggregation;
import org.neo4j.graphalgo.core.loading.construction.GraphFactory;
import org.neo4j.graphalgo.core.utils.ProgressTimer;
import org.neo4j.values.storable.NumberType;

import java.util.Optional;

import static org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.TOTAL_COST_KEY;

public abstract class ShortestPathMutateProc<ALGO extends Algorithm<ALGO, DijkstraResult>, CONFIG extends AlgoBaseConfig & MutateRelationshipConfig>
    extends MutateProc<ALGO, DijkstraResult, MutateResult, CONFIG> {

    @Override
    protected void updateGraphStore(AbstractResultBuilder<?> resultBuilder, ComputationResult<ALGO, DijkstraResult, CONFIG> computationResult) {
        var config = computationResult.config();
        var result = computationResult.result();

        var mutateRelationshipType = RelationshipType.of(config.mutateRelationshipType());

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(computationResult.graph())
            .addPropertyConfig(Aggregation.NONE, DefaultValue.forDouble())
            .orientation(Orientation.NATURAL)
            .tracker(allocationTracker())
            .build();

        Relationships relationships;

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            result.forEachPath(pathResult -> {
                relationshipsBuilder.addFromInternal(
                    pathResult.sourceNode(),
                    pathResult.targetNode(),
                    pathResult.totalCost()
                );
            });
            relationships = relationshipsBuilder.build();
            resultBuilder.withRelationshipsWritten(relationships.topology().elementCount());
        }

        computationResult
            .graphStore()
            .addRelationshipType(mutateRelationshipType,
                Optional.of(TOTAL_COST_KEY),
                Optional.of(NumberType.FLOATING_POINT),
                relationships
            );
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<ALGO, DijkstraResult, CONFIG> computeResult) {
        return new MutateResult.Builder()
            .withCreateMillis(computeResult.createMillis())
            .withComputeMillis(computeResult.computeMillis())
            .withConfig(computeResult.config());
    }
}
