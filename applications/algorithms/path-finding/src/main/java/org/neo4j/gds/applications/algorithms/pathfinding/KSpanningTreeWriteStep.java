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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.kspanningtree.KSpanningTreeWriteConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.spanningtree.SpanningTree;

class KSpanningTreeWriteStep implements WriteStep<SpanningTree, Void> {
    private final Log log;
    private final RequestScopedDependencies requestScopedDependencies;
    private final WriteContext writeContext;
    private final KSpanningTreeWriteConfig configuration;


    KSpanningTreeWriteStep(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        KSpanningTreeWriteConfig configuration
    ) {
        this.log = log;
        this.requestScopedDependencies = requestScopedDependencies;
        this.writeContext = writeContext;
        this.configuration = configuration;
    }

    @Override
    public Void execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        SpanningTree spanningTree,
        JobId jobId
    ) {
        var properties = new SpanningTreeBackedNodePropertyValues(spanningTree, graph.nodeCount());

        var progressTracker = new TaskProgressTracker(
            NodePropertyExporter.baseTask(AlgorithmLabel.KSpanningTree.asString(), graph.nodeCount()),
            log,
            configuration.writeConcurrency(),
            requestScopedDependencies.taskRegistryFactory()
        );

        var nodePropertyExporter = writeContext.nodePropertyExporterBuilder()
            .withIdMap(graph)
            .withTerminationFlag(requestScopedDependencies.terminationFlag())
            .withProgressTracker(progressTracker)
            .withResultStore(configuration.resolveResultStore(resultStore))
            .withJobId(configuration.jobId())
            .build();

        // effect
        nodePropertyExporter.write(configuration.writeProperty(), properties);

        // reporting
        // countsBuilder.withNodePropertiesWritten(...);
        return null;
    }
}
