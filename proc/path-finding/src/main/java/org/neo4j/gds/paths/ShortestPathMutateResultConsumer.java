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
import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.MutateRelationshipConfig;
import org.neo4j.gds.core.Aggregation;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.core.loading.construction.RelationshipsAndSchema;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.values.storable.NumberType;

import java.util.Optional;

import static org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.TOTAL_COST_KEY;

public class ShortestPathMutateResultConsumer<ALGO extends Algorithm<DijkstraResult>, CONFIG extends AlgoBaseConfig & MutateRelationshipConfig> extends MutateComputationResultConsumer<ALGO, DijkstraResult, CONFIG, MutateResult> {

    public ShortestPathMutateResultConsumer() {
        super((computationResult, executionContext) -> new MutateResult.Builder()
            .withPreProcessingMillis(computationResult.preProcessingMillis())
            .withComputeMillis(computationResult.computeMillis())
            .withConfig(computationResult.config()));
    }

    @Override
    protected void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, DijkstraResult, CONFIG> computationResult,
        ExecutionContext executionContext
    ) {
        var config = computationResult.config();
        var result = computationResult.result();

        var mutateRelationshipType = RelationshipType.of(config.mutateRelationshipType());

        var relationshipsBuilder = GraphFactory
            .initRelationshipsBuilder()
            .nodes(computationResult.graph())
            .addPropertyConfig(Aggregation.NONE, DefaultValue.forDouble())
            .orientation(Orientation.NATURAL)
            .build();

        RelationshipsAndSchema relationshipsAndSchema;

        try (ProgressTimer ignored = ProgressTimer.start(resultBuilder::withMutateMillis)) {
            result.forEachPath(pathResult -> {
                relationshipsBuilder.addFromInternal(pathResult.sourceNode(),
                    pathResult.targetNode(),
                    pathResult.totalCost()
                );
            });
            relationshipsAndSchema = relationshipsBuilder.build();
            resultBuilder.withRelationshipsWritten(relationshipsAndSchema.relationships().topology().elementCount());
        }

        computationResult
            .graphStore()
            .addRelationshipType(mutateRelationshipType,
                Optional.of(TOTAL_COST_KEY),
                Optional.of(NumberType.FLOATING_POINT),
                relationshipsAndSchema
            );
    }
}
