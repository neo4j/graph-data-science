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
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.config.MutateRelationshipConfig;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

import static org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig.TOTAL_COST_KEY;

class ShortestPathMutateStep implements MutateStep<PathFindingResult, RelationshipsWritten> {
    private final MutateRelationshipConfig configuration;

    ShortestPathMutateStep(MutateRelationshipConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        PathFindingResult result
    ) {
        var mutateRelationshipType = RelationshipType.of(configuration.mutateRelationshipType());

        var relationshipsBuilder = GraphFactory
            .initRelationshipsBuilder()
            .relationshipType(mutateRelationshipType)
            .nodes(graph)
            .addPropertyConfig(GraphFactory.PropertyConfig.of(TOTAL_COST_KEY))
            .orientation(Orientation.NATURAL)
            .build();

        SingleTypeRelationships relationships;

        result.forEachPath(pathResult -> relationshipsBuilder.addFromInternal(
            pathResult.sourceNode(),
            pathResult.targetNode(),
            pathResult.totalCost()
        ));

        relationships = relationshipsBuilder.build();

        // side effect
        graphStore.addRelationshipType(relationships);

        // result
        return new RelationshipsWritten(relationships.topology().elementCount());
    }
}
