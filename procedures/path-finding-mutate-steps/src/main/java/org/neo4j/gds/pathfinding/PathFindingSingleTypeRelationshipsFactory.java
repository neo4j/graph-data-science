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
package org.neo4j.gds.pathfinding;

import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.SingleTypeRelationshipsProducer;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;

 final class PathFindingSingleTypeRelationshipsFactory {
     private static final String TOTAL_COST_KEY = "totalCost";

     private PathFindingSingleTypeRelationshipsFactory() {}

     static SingleTypeRelationshipsProducer fromPathFindingResult(PathFindingResult result, Graph graph){

        return (mutateRelationshipType, mutateProperty) -> {
            var relationshipsBuilder = GraphFactory
                .initRelationshipsBuilder()
                .relationshipType(RelationshipType.of(mutateRelationshipType))
                .nodes(graph)
                .addPropertyConfig(GraphFactory.PropertyConfig.of(TOTAL_COST_KEY))
                .orientation(Orientation.NATURAL)
                .build();

            result.forEachPath(pathResult -> relationshipsBuilder.addFromInternal(
                pathResult.sourceNode(),
                pathResult.targetNode(),
                pathResult.totalCost()
            ));

            return relationshipsBuilder.build();
        };

    }

    static SingleTypeRelationshipsProducer fromBellmanFordResult(BellmanFordResult bellmanFordResult,Graph graph, boolean mutateNegativeCycles){

        var pathFindingResult = bellmanFordResult.shortestPaths();
        if (bellmanFordResult.containsNegativeCycle() && mutateNegativeCycles) {
            pathFindingResult = bellmanFordResult.negativeCycles();
        }
        return  fromPathFindingResult(pathFindingResult,graph);
    }
}
