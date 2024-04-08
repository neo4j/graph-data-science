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

import org.neo4j.gds.algorithms.RequestScopedDependencies;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.spanningtree.SpanningGraph;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;

class SteinerTreeWriteStep implements MutateOrWriteStep<SteinerTreeResult> {
    private final RequestScopedDependencies requestScopedDependencies;
    private final SteinerTreeWriteConfig configuration;

    SteinerTreeWriteStep(
        RequestScopedDependencies requestScopedDependencies,
        SteinerTreeWriteConfig configuration
    ) {
        this.requestScopedDependencies = requestScopedDependencies;
        this.configuration = configuration;
    }

    @Override
    public void execute(
        Graph graph,
        GraphStore graphStore,
        SteinerTreeResult steinerTreeResult,
        SideEffectProcessingCountsBuilder countsBuilder
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

        var relationshipExporter = requestScopedDependencies.getRelationshipExporterBuilder()
            .withGraph(spanningGraph)
            .withIdMappingOperator(spanningGraph::toOriginalNodeId)
            .withTerminationFlag(requestScopedDependencies.getTerminationFlag())
            .withProgressTracker(ProgressTracker.NULL_TRACKER)
            .withArrowConnectionInfo(
                configuration.arrowConnectionInfo(),
                graphStore.databaseInfo().remoteDatabaseId().map(DatabaseId::databaseName)
            )
            .withResultStore(configuration.resolveResultStore(graphStore.resultStore()))
            .build();

        relationshipExporter.write(configuration.writeRelationshipType(), configuration.writeProperty());

        countsBuilder.withRelationshipsWritten(steinerTreeResult.effectiveNodeCount() - 1);
    }
}
