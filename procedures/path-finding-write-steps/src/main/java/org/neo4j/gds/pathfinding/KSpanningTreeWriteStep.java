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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.logging.LoggerForProgressTrackingAdapter;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.TaskProgressTracker;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.function.Function;

// FIXME: This implementation is not what we would like it to be.
//  We need to abstract the write operation to a write service and not create the individual exporters here.
public class KSpanningTreeWriteStep implements WriteStep<SpanningTree, Void> {

    private final String writeProperty;
    private final Log log;
    private final WriteContext writeContext;
    private final Function<ResultStore, Optional<ResultStore>> resultStoreResolver;
    private final JobId jobId;
    private final Concurrency writeConcurrency;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;

    public KSpanningTreeWriteStep(
        String writeProperty,
        WriteContext writeContext,
        Function<ResultStore, Optional<ResultStore>> resultStoreResolver,
        JobId jobId,
        Concurrency writeConcurrency,
        Log log,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag
    ) {
        this.writeProperty = writeProperty;
        this.log = log;
        this.writeContext = writeContext;
        this.resultStoreResolver = resultStoreResolver;
        this.jobId = jobId;
        this.writeConcurrency = writeConcurrency;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationFlag = terminationFlag;
    }

    @Override
    public Void execute(
        Graph graph,
        GraphStore graphStore,
        ResultStore resultStore,
        SpanningTree spanningTree,
        JobId jobId
    ) {
        var properties = new KSpanningTreeBackedNodePropertyValues(spanningTree, graph.nodeCount());

        var progressTracker = TaskProgressTracker.create(
            NodePropertyExporter.baseTask(AlgorithmLabel.KSpanningTree.asString(), graph.nodeCount()),
            new LoggerForProgressTrackingAdapter(log),
            writeConcurrency,
            taskRegistryFactory
        );

        var nodePropertyExporter = writeContext.nodePropertyExporterBuilder()
            .withIdMap(graph)
            .withTerminationFlag(terminationFlag)
            .withProgressTracker(progressTracker)
            .withResultStore(resultStoreResolver.apply(resultStore))
            .withJobId(this.jobId)
            .build();

        // effect
        nodePropertyExporter.write(writeProperty, properties);

        // reporting
        // countsBuilder.withNodePropertiesWritten(...);
        return null;
    }
}
