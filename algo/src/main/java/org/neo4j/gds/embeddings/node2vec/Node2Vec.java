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
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.core.EmbeddingUtils;
import org.neo4j.gds.traversal.RandomWalkCompanion;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class Node2Vec extends Algorithm<Node2VecModel.Result> {

    private final Graph graph;
    private final int concurrency;
    private final WalkParameters walkParameters;
    private final List<Long> sourceNodes;
    private final Optional<Long> maybeRandomSeed;
    private final TrainParameters trainParameters;
    private final int walkBufferSize;


    public static MemoryEstimation memoryEstimation(int walksPerNode, int walkLength, int embeddingDimension) {
        return MemoryEstimations.builder(Node2Vec.class.getSimpleName())
            .perNode("random walks", (nodeCount) -> {
                var numberOfRandomWalks = nodeCount * walksPerNode;
                var randomWalkMemoryUsage = MemoryUsage.sizeOfLongArray(walkLength);
                return HugeObjectArray.memoryEstimation(numberOfRandomWalks, randomWalkMemoryUsage);
            })
            .add("probability cache", RandomWalkProbabilities.memoryEstimation())
            .add("model", Node2VecModel.memoryEstimation(embeddingDimension))
            .build();
    }

    static Node2Vec create(
        Graph graph,
        int concurrency,
        WalkParameters walkParameters,
        TrainParameters trainParameters,
        ProgressTracker progressTracker
    ) {
        return create(graph, concurrency, Optional.empty(), walkParameters, trainParameters, progressTracker);
    }

    static Node2Vec create(
        Graph graph,
        int concurrency,
        Optional<Long> maybeRandomSeed,
        WalkParameters walkParameters,
        TrainParameters trainParameters,
        ProgressTracker progressTracker
    ) {
        return new Node2Vec(
            graph,
            concurrency,
            List.of(),
            maybeRandomSeed,
            1000,
            walkParameters,
            trainParameters,
            progressTracker
        );
    }

    public Node2Vec(
        Graph graph,
        int concurrency,
        List<Long> sourceNodes,
        Optional<Long> maybeRandomSeed,
        int walkBufferSize,
        WalkParameters walkParameters,
        TrainParameters trainParameters,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.concurrency = concurrency;
        this.walkParameters = walkParameters;
        this.walkBufferSize = walkBufferSize;
        this.sourceNodes = sourceNodes;
        this.maybeRandomSeed = maybeRandomSeed;
        this.trainParameters = trainParameters;
    }

    @Override
    public Node2VecModel.Result compute() {
        progressTracker.beginSubTask("Node2Vec");

        if (graph.hasRelationshipProperty()) {
            EmbeddingUtils.validateRelationshipWeightPropertyValue(
                graph,
                concurrency,
                weight -> weight >= 0,
                "Node2Vec only supports non-negative weights.",
                DefaultPool.INSTANCE
            );
        }

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            graph.nodeCount(),
            concurrency,
            walkParameters.positiveSamplingFactor,
            walkParameters.negativeSamplingExponent
        );
        var walks = new CompressedRandomWalks(graph.nodeCount() * walkParameters.walksPerNode);

        progressTracker.beginSubTask("RandomWalk");

        var tasks = walkTasks(
            walks,
            probabilitiesBuilder,
            graph,
            maybeRandomSeed,
            concurrency,
            sourceNodes,
            walkParameters,
            walkBufferSize,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        progressTracker.beginSubTask("create walks");
        RunWithConcurrency.builder().concurrency(concurrency).tasks(tasks).run();
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
        int concurrency,
        List<Long> sourceNodes,
        WalkParameters walkParameters,
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
            concurrency,
            executorService,
            progressTracker
        );

        AtomicLong index = new AtomicLong();
        for (int i = 0; i < concurrency; ++i) {
            tasks.add(new Node2VecRandomWalkTask(
                graph.concurrentCopy(),
                nextNodeSupplier,
                walkParameters.walksPerNode,
                cumulativeWeightsSupplier,
                progressTracker,
                terminationFlag,
                index,
                compressedRandomWalks,
                randomWalkPropabilitiesBuilder,
                walkBufferSize,
                randomSeed,
                walkParameters.walkLength,
                walkParameters.returnFactor,
                walkParameters.inOutFactor
            ));
        }
        return tasks;
    }
}
