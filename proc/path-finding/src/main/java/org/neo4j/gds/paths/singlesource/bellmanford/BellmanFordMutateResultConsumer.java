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
package org.neo4j.gds.paths.singlesource.bellmanford;

import org.neo4j.gds.MutateComputationResultConsumer;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.paths.bellmanford.BellmanFord;
import org.neo4j.gds.paths.bellmanford.BellmanFordMutateConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.result.AbstractResultBuilder;

import static org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.TOTAL_COST_KEY;

public class BellmanFordMutateResultConsumer extends MutateComputationResultConsumer<BellmanFord, BellmanFordResult, BellmanFordMutateConfig, BellmanFordMutateResult> {

    BellmanFordMutateResultConsumer() {
        super((computationResult, executionContext) -> new BellmanFordMutateResult.Builder()
            .withContainsNegativeCycle(computationResult.result().containsNegativeCycle())
            .withPreProcessingMillis(computationResult.preProcessingMillis())
            .withComputeMillis(computationResult.computeMillis())
            .withConfig(computationResult.config())
        );
    }

    @Override
    protected void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<BellmanFord, BellmanFordResult, BellmanFordMutateConfig> computationResult,
        ExecutionContext executionContext
    ) {
        var config = computationResult.config();
        var result = computationResult.result();

        var mutateRelationshipType = RelationshipType.of(config.mutateRelationshipType());

        var relationshipsBuilder = GraphFactory
            .initRelationshipsBuilder()
            .relationshipType(mutateRelationshipType)
            .nodes(computationResult.graph())
            .addPropertyConfig(GraphFactory.PropertyConfig.of(TOTAL_COST_KEY))
            .orientation(Orientation.NATURAL)
            .build();

        SingleTypeRelationships relationships;

        result.shortestPaths().forEachPath(pathResult -> {
            relationshipsBuilder.addFromInternal(
                pathResult.sourceNode(),
                pathResult.targetNode(),
                pathResult.totalCost()
            );
        });

        relationships = relationshipsBuilder.build();
        resultBuilder.withRelationshipsWritten(relationships.topology().elementCount());

        computationResult
            .graphStore()
            .addRelationshipType(relationships);
    }
}
