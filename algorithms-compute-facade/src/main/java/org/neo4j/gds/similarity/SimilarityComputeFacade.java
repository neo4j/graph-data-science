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
package org.neo4j.gds.similarity;

import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.SimilarityAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.similarity.filteredknn.FilteredKNNFactory;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnParameters;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnContext;
import org.neo4j.gds.similarity.knn.KnnNeighborFilterFactory;
import org.neo4j.gds.similarity.knn.KnnParameters;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.SimilarityFunction;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityParameters;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class SimilarityComputeFacade {
    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;

    // Request scope dependencies
    private final TerminationFlag terminationFlag;

    // Local dependencies
    private final SimilarityAlgorithmTasks tasks = new SimilarityAlgorithmTasks();

    public SimilarityComputeFacade(
        AsyncAlgorithmCaller algorithmCaller,
        ProgressTrackerFactory progressTrackerFactory,
        TerminationFlag terminationFlag
    ) {
        this.algorithmCaller = algorithmCaller;
        this.progressTrackerFactory = progressTrackerFactory;
        this.terminationFlag = terminationFlag;
    }

    public CompletableFuture<TimedAlgorithmResult<KnnResult>> knn(
        Graph graph,
        KnnParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(KnnResult.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            tasks.knn(graph,parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        // Create the algorithm
        var knn = Knn.create(
            graph,
            parameters,
            new SimilarityFunction(SimilarityComputer.ofProperties(graph, parameters.nodePropertySpecs())),
            new KnnNeighborFilterFactory(graph.nodeCount()),
            Optional.empty(),
            new KnnContext(progressTracker),
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            knn::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<FilteredKnnResult>> filteredKnn(
        Graph graph,
        FilteredKnnParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(FilteredKnnResult.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            tasks.filteredKnn(graph,parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );
        var knnContext = new KnnContext(progressTracker);

        // Create the algorithm
        var filteredKnn = FilteredKNNFactory.create(graph, parameters, knnContext,terminationFlag);

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            filteredKnn::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<NodeSimilarityResult>> nodeSimilarity(
        Graph graph,
        NodeSimilarityParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        // If the input graph is empty return a completed future with empty result
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(NodeSimilarityResult.EMPTY));
        }

        // Create ProgressTracker
        var progressTracker = progressTrackerFactory.create(
            tasks.nodeSimilarity(graph,parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );


        var nodeSimilarity = NodeSimilarity.create(
            graph,
            parameters,
            DefaultPool.INSTANCE,
            progressTracker,
            NodeFilter.ALLOW_EVERYTHING,
            NodeFilter.ALLOW_EVERYTHING,
            terminationFlag
        );

        // Submit the algorithm for async computation
        return algorithmCaller.run(
            nodeSimilarity::compute,
            jobId
        );
    }

}
