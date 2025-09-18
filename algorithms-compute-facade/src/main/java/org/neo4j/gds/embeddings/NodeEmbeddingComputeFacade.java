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
package org.neo4j.gds.embeddings;

import org.neo4j.gds.NodeEmbeddingsAlgorithmTasks;
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.embeddings.fastrp.FastRP;
import org.neo4j.gds.embeddings.fastrp.FastRPParameters;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.hashgnn.HashGNN;
import org.neo4j.gds.embeddings.hashgnn.HashGNNParameters;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;
import org.neo4j.gds.embeddings.node2vec.Node2Vec;
import org.neo4j.gds.embeddings.node2vec.Node2VecParameters;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class NodeEmbeddingComputeFacade {

    // Global dependencies
    // This is created with its own ExecutorService workerPool,
    // which determines how many algorithms can run in parallel.
    private final AsyncAlgorithmCaller algorithmCaller;
    private final ProgressTrackerFactory progressTrackerFactory;

    // Request scope dependencies
    private final TerminationFlag terminationFlag;

    // Local dependencies
    private final NodeEmbeddingsAlgorithmTasks tasks = new NodeEmbeddingsAlgorithmTasks();


    public NodeEmbeddingComputeFacade(
        AsyncAlgorithmCaller algorithmCaller,
        ProgressTrackerFactory progressTrackerFactory,
        TerminationFlag terminationFlag
    ) {
        this.algorithmCaller = algorithmCaller;
        this.progressTrackerFactory = progressTrackerFactory;
        this.terminationFlag = terminationFlag;
    }


    public CompletableFuture<TimedAlgorithmResult<FastRPResult>> fastRP(
        Graph graph,
        FastRPParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(FastRPResult.empty()));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.fastRP(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var featureExtractors = FeatureExtraction.propertyExtractors(graph, parameters.featureProperties());

        var fastRP = new FastRP(
            graph,
            parameters,
            10_000,
            featureExtractors,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            fastRP::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<HashGNNResult>> hashGnn(
        Graph graph,
        HashGNNParameters parameters,
        List<String> relationshipTypes,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(HashGNNResult.empty()));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.hashGNN(graph, parameters, relationshipTypes),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var hashGNN = new HashGNN(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            hashGNN::compute,
            jobId
        );
    }

    public CompletableFuture<TimedAlgorithmResult<Node2VecResult>> node2Vec(
        Graph graph,
        Node2VecParameters parameters,
        JobId jobId,
        boolean logProgress
    ) {
        if (graph.isEmpty()) {
            return CompletableFuture.completedFuture(TimedAlgorithmResult.empty(Node2VecResult.empty()));
        }

        var progressTracker = progressTrackerFactory.create(
            tasks.node2Vec(graph, parameters),
            jobId,
            parameters.concurrency(),
            logProgress
        );

        var node2Vec = Node2Vec.create(
            graph,
            parameters,
            progressTracker,
            terminationFlag
        );

        return algorithmCaller.run(
            node2Vec::compute,
            jobId
        );
    }
}
