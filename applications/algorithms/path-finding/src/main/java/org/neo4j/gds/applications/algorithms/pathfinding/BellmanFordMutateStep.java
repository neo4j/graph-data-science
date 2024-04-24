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
package org.neo4j.gds.applications.algorithms.pathfinding;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateOrWriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.paths.bellmanford.BellmanFordMutateConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;

import static org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.TOTAL_COST_KEY;

class BellmanFordMutateStep implements MutateOrWriteStep<BellmanFordResult, RelationshipsWritten> {
    private final BellmanFordMutateConfig configuration;

    BellmanFordMutateStep(BellmanFordMutateConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        BellmanFordResult result
    ) {
        var mutateRelationshipType = RelationshipType.of(configuration.mutateRelationshipType());

        var relationshipsBuilder = GraphFactory
            .initRelationshipsBuilder()
            .relationshipType(mutateRelationshipType)
            .nodes(graph)
            .addPropertyConfig(GraphFactory.PropertyConfig.of(TOTAL_COST_KEY))
            .orientation(Orientation.NATURAL)
            .build();

        var pathFindingResult = result.shortestPaths();
        if (result.containsNegativeCycle() && configuration.mutateNegativeCycles()) {
            pathFindingResult = result.negativeCycles();
        }

        pathFindingResult.forEachPath(pathResult -> relationshipsBuilder.addFromInternal(
            pathResult.sourceNode(),
            pathResult.targetNode(),
            pathResult.totalCost()
        ));

        var relationships = relationshipsBuilder.build();

        // effect
        graphStore.addRelationshipType(relationships);

        // reporting
        return new RelationshipsWritten(relationships.topology().elementCount());
    }
}
