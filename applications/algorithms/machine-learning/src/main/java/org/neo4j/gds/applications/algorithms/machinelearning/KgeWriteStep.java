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
package org.neo4j.gds.applications.algorithms.machinelearning;

import org.neo4j.gds.algorithms.machinelearning.KGEPredictResult;
import org.neo4j.gds.algorithms.machinelearning.KGEPredictWriteConfig;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.similarity.nodesim.TopKGraph;

class KgeWriteStep implements WriteStep<KGEPredictResult, RelationshipsWritten> {
    private final Log log;
    private final RequestScopedDependencies requestScopedDependencies;
    private final KGEPredictWriteConfig configuration;
    private final WriteContext writeContext;

    KgeWriteStep(
        Log log,
        RequestScopedDependencies requestScopedDependencies,
        KGEPredictWriteConfig configuration,
        WriteContext writeContext
    ) {
        this.log = log;
        this.requestScopedDependencies = requestScopedDependencies;
        this.configuration = configuration;
        this.writeContext = writeContext;
    }

    @Override
    public RelationshipsWritten execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        KGEPredictResult result,
        JobId jobId
    ) {
        var topKGraph = new TopKGraph(graph, result.topKMap());

        var task = NodePropertyExporter.baseTask(AlgorithmLabel.KGE.asString(), graph.nodeCount());
        var progressTracker = new TaskProgressTracker(
            task,
            log,
            RelationshipExporterBuilder.TYPED_DEFAULT_WRITE_CONCURRENCY,
            requestScopedDependencies.taskRegistryFactory()
        );

        var relationshipExporter = writeContext.relationshipExporterBuilder()
            .withGraph(topKGraph)
            .withIdMappingOperator(topKGraph::toOriginalNodeId)
            .withJobId(jobId)
            .withResultStore(configuration.resolveResultStore(resultStore))
            .withProgressTracker(progressTracker)
            .withTerminationFlag(requestScopedDependencies.terminationFlag())
            .build();

        relationshipExporter.write(configuration.writeRelationshipType(), configuration.writeProperty());

        return new RelationshipsWritten(topKGraph.relationshipCount());
    }
}
