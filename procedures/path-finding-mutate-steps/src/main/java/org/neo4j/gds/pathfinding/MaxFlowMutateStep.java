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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.MutateStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.maxflow.FlowResult;

public class MaxFlowMutateStep implements MutateStep<FlowResult, RelationshipsWritten> {
    private final String mutateRelationshipType;
    private final String mutateProperty;
    private final MutateRelationshipService mutateRelationshipService;

    public MaxFlowMutateStep(
        String mutateRelationshipType,
        String mutateProperty,
        MutateRelationshipService mutateRelationshipService
    ) {
        this.mutateRelationshipType = mutateRelationshipType;
        this.mutateProperty = mutateProperty;
        this.mutateRelationshipService = mutateRelationshipService;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        FlowResult result
    ) {

        var relationshipsBuilder = GraphFactory
            .initRelationshipsBuilder()
            .relationshipType(RelationshipType.of(mutateRelationshipType))
            .nodes(graph)
            .addPropertyConfig(GraphFactory.PropertyConfig.builder()
                .propertyKey(mutateProperty)
                .build())
            .orientation(Orientation.NATURAL)
            .build();

        for(long idx = 0; idx < result.flow().size(); idx++){
            var rel = result.flow().get(idx);
            relationshipsBuilder.addFromInternal(rel.sourceId(), rel.targetId(), rel.flow());
        }

        var relationships = relationshipsBuilder.build();

        return mutateRelationshipService.mutate(graphStore, relationships);
    }
}
