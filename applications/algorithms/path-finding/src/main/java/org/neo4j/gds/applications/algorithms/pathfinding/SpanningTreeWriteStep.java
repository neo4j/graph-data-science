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
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.spanningtree.SpanningGraph;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;
import org.neo4j.gds.termination.TerminationFlag;

import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.SPANNING_TREE;

class SpanningTreeWriteStep implements MutateOrWriteStep<SpanningTree> {
    private final Log log;
    private final RelationshipExporterBuilder relationshipExporterBuilder;
    private final SpanningTreeWriteConfig configuration;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;

    SpanningTreeWriteStep(
        Log log,
        RelationshipExporterBuilder relationshipExporterBuilder,
        TerminationFlag terminationFlag,
        TaskRegistryFactory taskRegistryFactory, SpanningTreeWriteConfig configuration
    ) {
        this.log = log;
        this.relationshipExporterBuilder = relationshipExporterBuilder;
        this.terminationFlag = terminationFlag;
        this.configuration = configuration;
        this.taskRegistryFactory = taskRegistryFactory;
    }

    @Override
    public void execute(
        Graph graph,
        GraphStore graphStore,
        SpanningTree result,
        SideEffectProcessingCountsBuilder countsBuilder
    ) {
        var spanningGraph = new SpanningGraph(graph, result);

        var progressTracker = new TaskProgressTracker(
            NodePropertyExporter.baseTask(SPANNING_TREE, graph.nodeCount()),
            (org.neo4j.logging.Log) log.getNeo4jLog(),
            configuration.writeConcurrency(),
            taskRegistryFactory
        );

        var relationshipExporter = relationshipExporterBuilder
            .withGraph(spanningGraph)
            .withIdMappingOperator(spanningGraph::toOriginalNodeId)
            .withTerminationFlag(terminationFlag)
            .withProgressTracker(progressTracker)
            .withArrowConnectionInfo(
                configuration.arrowConnectionInfo(),
                graphStore.databaseInfo().remoteDatabaseId().map(DatabaseId::databaseName)
            )
            .withResultStore(configuration.resolveResultStore(graphStore.resultStore()))
            .build();

        // effect
        relationshipExporter.write(configuration.writeRelationshipType(), configuration.writeProperty());

        // reporting
        countsBuilder.withRelationshipsWritten(result.effectiveNodeCount() - 1);
    }
}
