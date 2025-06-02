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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.spanningtree.SpanningGraph;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;

class SteinerTreeWriteStep implements WriteStep<SteinerTreeResult, RelationshipsWritten> {
    private final SteinerTreeWriteConfig configuration;
    private final WriteRelationshipService writeRelationshipService;

    SteinerTreeWriteStep(
        WriteRelationshipService writeRelationshipService,
        SteinerTreeWriteConfig configuration
    ) {
        this.configuration = configuration;
        this.writeRelationshipService = writeRelationshipService;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        SteinerTreeResult steinerTreeResult,
        JobId jobId
    ) {
        var sourceNodeId = configuration.sourceNode();

        var spanningTree = new SpanningTree(
            graph.toMappedNodeId(sourceNodeId),
            graph.nodeCount(),
            steinerTreeResult.effectiveNodeCount(),
            steinerTreeResult.parentArray(),
            nodeId -> steinerTreeResult.relationshipToParentCost().get(nodeId),
            steinerTreeResult.totalCost()
        );
        var spanningGraph = new SpanningGraph(graph, spanningTree);

        return writeRelationshipService.writeFromGraph(
             configuration.writeRelationshipType(),
             configuration.writeProperty(),
             spanningGraph,
             graph,
             "SteinerWrite",
             configuration.resolveResultStore(resultStore),
             (a,b,c)-> true,
             configuration.jobId()
         );
    }
}
