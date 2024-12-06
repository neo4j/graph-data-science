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
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.pcst.PCSTMutateConfig;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;

import java.util.stream.LongStream;

class PrizeCollectingSteinerTreeMutateStep implements MutateStep<PrizeSteinerTreeResult, RelationshipsWritten> {
    private final PCSTMutateConfig configuration;

    PrizeCollectingSteinerTreeMutateStep(PCSTMutateConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        PrizeSteinerTreeResult treeResult
    ) {
        var mutateRelationshipType = RelationshipType.of(configuration.mutateRelationshipType());

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(graph)
            .relationshipType(mutateRelationshipType)
            .addPropertyConfig(GraphFactory.PropertyConfig.of(configuration.mutateProperty()))
            .orientation(Orientation.NATURAL)
            .build();

        var parentArray = treeResult.parentArray();
        var costArray = treeResult.relationshipToParentCost();
        LongStream.range(0, graph.nodeCount())
            .filter(nodeId -> parentArray.get(nodeId) != PrizeSteinerTreeResult.PRUNED )
            .filter(nodeId -> parentArray.get(nodeId) != PrizeSteinerTreeResult.ROOT )
            .forEach(nodeId -> {
                var parentId = parentArray.get(nodeId);
                    relationshipsBuilder.addFromInternal(parentId, nodeId, costArray.get(nodeId));

            });

        var relationships = relationshipsBuilder.build();

        // the effect
        graphStore.addRelationshipType(relationships);

        // the reporting
        return new RelationshipsWritten(treeResult.effectiveNodeCount() - 1);
    }
}
