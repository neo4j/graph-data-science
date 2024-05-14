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

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.MutateOrWriteStep;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.spanningtree.SpanningGraph;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;

class SpanningTreeWriteStep implements MutateOrWriteStep<SpanningTree, RelationshipsWritten> {
    private final Log log;
    private final RequestScopedDependencies requestScopedDependencies;
    private final SpanningTreeWriteConfig configuration;

    SpanningTreeWriteStep(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        SpanningTreeWriteConfig configuration
    ) {
        this.log = log;
        this.requestScopedDependencies = requestScopedDependencies;
        this.configuration = configuration;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        SpanningTree result,
        JobId jobId
    ) {
        var spanningGraph = new SpanningGraph(graph, result);

        var progressTracker = new TaskProgressTracker(
            NodePropertyExporter.baseTask(LabelForProgressTracking.SpanningTree.value, graph.nodeCount()),
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            configuration.writeConcurrency(),
            requestScopedDependencies.getTaskRegistryFactory()
        );

        var relationshipExporter = requestScopedDependencies.getRelationshipExporterBuilder()
            .withGraph(spanningGraph)
            .withIdMappingOperator(spanningGraph::toOriginalNodeId)
            .withTerminationFlag(requestScopedDependencies.getTerminationFlag())
            .withProgressTracker(progressTracker)
            .withArrowConnectionInfo(
                configuration.arrowConnectionInfo(),
                graphStore.databaseInfo().remoteDatabaseId().map(DatabaseId::databaseName)
            )
            .withResultStore(configuration.resolveResultStore(resultStore))
            .withJobId(configuration.jobId())
            .build();

        // effect
        relationshipExporter.write(configuration.writeRelationshipType(), configuration.writeProperty());

        // reporting
        return new RelationshipsWritten(result.effectiveNodeCount() - 1);
    }
}
