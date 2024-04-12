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
import org.neo4j.gds.applications.algorithms.machinery.MutateOrWriteStep;
import org.neo4j.gds.core.loading.construction.GraphFactory;
import org.neo4j.gds.steiner.ShortestPathsSteinerAlgorithm;
import org.neo4j.gds.steiner.SteinerTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;

import java.util.stream.LongStream;

class SteinerTreeMutateStep implements MutateOrWriteStep<SteinerTreeResult, RelationshipsWritten> {
    private final SteinerTreeMutateConfig configuration;

    SteinerTreeMutateStep(SteinerTreeMutateConfig configuration) {
        this.configuration = configuration;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        SteinerTreeResult steinerTreeResult
    ) {
        var mutateRelationshipType = RelationshipType.of(configuration.mutateRelationshipType());

        var relationshipsBuilder = GraphFactory.initRelationshipsBuilder()
            .nodes(graph)
            .relationshipType(mutateRelationshipType)
            .addPropertyConfig(GraphFactory.PropertyConfig.of(configuration.mutateProperty()))
            .orientation(Orientation.NATURAL)
            .build();

        var parentArray = steinerTreeResult.parentArray();
        var costArray = steinerTreeResult.relationshipToParentCost();
        LongStream.range(0, graph.nodeCount())
            .filter(nodeId -> parentArray.get(nodeId) != ShortestPathsSteinerAlgorithm.PRUNED)
            .forEach(nodeId -> {
                var sourceNodeId = (configuration.sourceNode() == graph.toOriginalNodeId(nodeId)) ?
                    nodeId :
                    parentArray.get(nodeId);

                if (nodeId != sourceNodeId) {
                    relationshipsBuilder.addFromInternal(sourceNodeId, nodeId, costArray.get(nodeId));
                }
            });

        var relationships = relationshipsBuilder.build();

        // the effect
        graphStore.addRelationshipType(relationships);

        // the reporting
        return new RelationshipsWritten(steinerTreeResult.effectiveNodeCount() - 1);
    }
}
