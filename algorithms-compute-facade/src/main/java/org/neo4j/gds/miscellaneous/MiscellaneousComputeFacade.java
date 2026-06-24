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
package org.neo4j.gds.miscellaneous;

import org.neo4j.gds.MiscellaneousAlgorithmsTasks;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.collapsepath.CollapsePathParameters;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.SingleTypeRelationships;
import org.neo4j.gds.indexInverse.InverseRelationships;
import org.neo4j.gds.indexinverse.InverseRelationshipsParameters;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.scaleproperties.ScaleProperties;
import org.neo4j.gds.scaleproperties.ScalePropertiesParameters;
import org.neo4j.gds.scaleproperties.ScalePropertiesResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.undirected.ToUndirected;
import org.neo4j.gds.undirected.ToUndirectedParameters;
import org.neo4j.gds.walking.CollapsePath;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MiscellaneousComputeFacade {

    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;

    // Request scope dependencies
    private final TerminationFlag terminationFlag;

    public MiscellaneousComputeFacade(
        AsyncAlgorithmCaller algorithmCaller,
        ProgressTrackerFactory progressTrackerFactory,
        TerminationFlag terminationFlag
    ) {
        this.algorithmCaller = algorithmCaller;
        this.progressTrackerFactory = progressTrackerFactory;
        this.terminationFlag = terminationFlag;
    }

    public CompletableFuture<TimedAlgorithmResult<SingleTypeRelationships>> collapsePath(
        GraphStore graphStore,
        CollapsePathParameters parameters,
        JobId jobId
    ) {
        if (graphStore.nodeCount() == 0) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(SingleTypeRelationships.EMPTY));
        }

        var collapsePath =  CollapsePath.create(
            graphStore,
            parameters,
            DefaultPool.INSTANCE
        );

        return algorithmCaller.run(
            collapsePath::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<Map<RelationshipType, SingleTypeRelationships>>> indexInverse(
        GraphStore graphStore,
        InverseRelationshipsParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {

        if (graphStore.nodeCount() == 0) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(Map.of()));
        }

        var progressTracker = progressTrackerFactory.create(
            MiscellaneousAlgorithmsTasks.inverseIndex(graphStore.nodeCount(), parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var inverseRelationships = new InverseRelationships(
            graphStore,
            parameters,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithmCaller.run(
            inverseRelationships::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<ScalePropertiesResult>> scaleProperties(
        Graph graph,
        ScalePropertiesParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(ScalePropertiesResult.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            MiscellaneousAlgorithmsTasks.scaleProperties(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var scaleProperties = new ScaleProperties(
            graph,
            parameters,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );
        return algorithmCaller.run(
            scaleProperties::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<SingleTypeRelationships>> toUndirected(
        GraphStore graphStore,
        ToUndirectedParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graphStore.nodeCount() == 0) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(SingleTypeRelationships.EMPTY));
        }

        var progressTracker = progressTrackerFactory.create(
            MiscellaneousAlgorithmsTasks.toUndirected(graphStore, parameters.concurrency()),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var toUndirected = new ToUndirected(
            graphStore,
            parameters,
            progressTracker,
            DefaultPool.INSTANCE,
            terminationFlag
        );

        return algorithmCaller.run(
            toUndirected::compute,
            jobId
        );
    }


}
