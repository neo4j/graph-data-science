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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.EmbeddingUtils;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.RandomWalkCompanion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class Node2Vec extends Algorithm<Node2VecResult> {

    private final Graph graph;
    private final Concurrency concurrency;
    private final SamplingWalkParameters samplingWalkParameters;
    private final List<Long> sourceNodes;
    private final Optional<Long> maybeRandomSeed;
    private final TrainParameters trainParameters;
    private final int walkBufferSize;


    public Node2Vec(
        Graph graph,
        Concurrency concurrency,
        List<Long> sourceNodes,
        Optional<Long> maybeRandomSeed,
        int walkBufferSize,
        Node2VecParameters node2VecParameters,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = concurrency;
        this.samplingWalkParameters = node2VecParameters.samplingWalkParameters();
        this.walkBufferSize = walkBufferSize;
        this.sourceNodes = sourceNodes;
        this.maybeRandomSeed = maybeRandomSeed;
        this.trainParameters = node2VecParameters.trainParameters();
    }

    @Override
    public Node2VecResult compute() {
        progressTracker.beginSubTask("Node2Vec");

        if (graph.hasRelationshipProperty()) {
            EmbeddingUtils.validateRelationshipWeightPropertyValue(
                graph,
                concurrency.value(),
                weight -> weight >= 0,
                "Node2Vec only supports non-negative weights.",
                DefaultPool.INSTANCE
            );
        }

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            graph.nodeCount(),
            concurrency,
            samplingWalkParameters.positiveSamplingFactor(),
            samplingWalkParameters.negativeSamplingExponent()
        );
        var walks = new CompressedRandomWalks(graph.nodeCount() * samplingWalkParameters.walksPerNode());

        progressTracker.beginSubTask("RandomWalk");

        var tasks = walkTasks(
            walks,
            probabilitiesBuilder,
            graph,
            maybeRandomSeed,
            concurrency,
            sourceNodes,
            samplingWalkParameters,
            walkBufferSize,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        progressTracker.beginSubTask("create walks");
        RunWithConcurrency.builder().concurrency(concurrency.value()).tasks(tasks).run();
        walks.setMaxWalkLength(tasks.stream()
            .map(Node2VecRandomWalkTask::maxWalkLength)
            .max(Integer::compareTo)
            .orElse(0));

        walks.setSize(tasks.stream()
            .map(task -> (1 + task.maxIndex()))
            .max(Long::compareTo)
            .orElse(0L));

        progressTracker.endSubTask("create walks");
        progressTracker.endSubTask("RandomWalk");

        var node2VecModel = new Node2VecModel(
            graph::toOriginalNodeId,
            graph.nodeCount(),
            trainParameters,
            concurrency,
            maybeRandomSeed,
            walks,
            probabilitiesBuilder.build(),
            progressTracker
        );

        var result = node2VecModel.train();

        progressTracker.endSubTask("Node2Vec");
        return result;
    }

    private List<Node2VecRandomWalkTask> walkTasks(
        CompressedRandomWalks compressedRandomWalks,
        RandomWalkProbabilities.Builder randomWalkPropabilitiesBuilder,
        Graph graph,
        Optional<Long> maybeRandomSeed,
        Concurrency concurrency,
        List<Long> sourceNodes,
        SamplingWalkParameters samplingWalkParameters,
        int walkBufferSize,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        List<Node2VecRandomWalkTask> tasks = new ArrayList<>();
        var randomSeed = maybeRandomSeed.orElseGet(() -> new Random().nextLong());
        var nextNodeSupplier = RandomWalkCompanion.nextNodeSupplier(graph, sourceNodes);
        var cumulativeWeightsSupplier = RandomWalkCompanion.cumulativeWeights(
            graph,
            concurrency.value(),
            executorService,
            progressTracker
        );

        AtomicLong index = new AtomicLong();
        for (int i = 0; i < concurrency.value(); ++i) {
            tasks.add(new Node2VecRandomWalkTask(
                graph.concurrentCopy(),
                nextNodeSupplier,
                samplingWalkParameters.walksPerNode(),
                cumulativeWeightsSupplier,
                progressTracker,
                terminationFlag,
                index,
                compressedRandomWalks,
                randomWalkPropabilitiesBuilder,
                walkBufferSize,
                randomSeed,
                samplingWalkParameters.walkLength(),
                samplingWalkParameters.returnFactor(),
                samplingWalkParameters.inOutFactor()
            ));
        }
        return tasks;
    }
}
