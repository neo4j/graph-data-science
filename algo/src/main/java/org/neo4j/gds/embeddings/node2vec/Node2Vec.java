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
import org.neo4j.gds.traversal.WalkParameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class Node2Vec extends Algorithm<Node2VecModel.Result> {

    private final Graph graph;
    private final double positiveSamplingFactor;
    private final double negativeSamplingExponent;
    private final int concurrency;
    private final WalkParameters walkParameters;
    private final List<Long> sourceNodes;
    private final Optional<Long> maybeRandomSeed;
    private final double initialLearningRate;
    private final double minLearningRate;
    private final int iterations;
    private final int embeddingDimension;
    private final int windowSize;
    private final int negativeSamplingRate;
    private final Node2VecBaseConfig.EmbeddingInitializer embeddingInitializer;


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

    static Node2Vec create(Graph graph, Node2VecBaseConfig config, ProgressTracker progressTracker) {
        return new Node2Vec(
            graph,
            config.concurrency(),
            config.positiveSamplingFactor(),
            config.negativeSamplingExponent(),
            config.walkParameters(),
            config.sourceNodes(),
            config.randomSeed(),
            progressTracker,
            config.initialLearningRate(),
            config.minLearningRate(),
            config.iterations(),
            config.embeddingDimension(),
            config.windowSize(),
            config.negativeSamplingRate(),
            config.embeddingInitializer()
        );
    }

    public Node2Vec(
        Graph graph,
        int concurrency,
        double positiveSamplingFactor,
        double negativeSamplingExponent,
        WalkParameters walkParameters,
        List<Long> sourceNodes,
        Optional<Long> maybeRandomSeed,
        ProgressTracker progressTracker,
        // train params
        double initialLearningRate,
        double minLearningRate,
        int iterations,
        int embeddingDimension,
        int windowSize,
        int negativeSamplingRate,
        Node2VecBaseConfig.EmbeddingInitializer embeddingInitializer

    ) {
        super(progressTracker);
        this.graph = graph;
        this.positiveSamplingFactor = positiveSamplingFactor;
        this.negativeSamplingExponent = negativeSamplingExponent;
        this.concurrency = concurrency;
        this.walkParameters = walkParameters;
        this.sourceNodes = sourceNodes;
        this.maybeRandomSeed = maybeRandomSeed;
        this.initialLearningRate = initialLearningRate;
        this.minLearningRate = minLearningRate;
        this.iterations = iterations;
        this.embeddingDimension = embeddingDimension;
        this.windowSize = windowSize;
        this.negativeSamplingRate = negativeSamplingRate;
        this.embeddingInitializer = embeddingInitializer;
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
            positiveSamplingFactor,
            negativeSamplingExponent,
            concurrency
        );
        var walks = new CompressedRandomWalks(graph.nodeCount() * walkParameters.walksPerNode);

        progressTracker.beginSubTask("RandomWalk");

        var tasks = tasks(
            walks,
            probabilitiesBuilder,
            graph,
            maybeRandomSeed,
            concurrency,
            sourceNodes,
            walkParameters,
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
            initialLearningRate,
            minLearningRate,
            iterations,
            embeddingDimension,
            windowSize,
            negativeSamplingRate,
            embeddingInitializer,
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

    private List<Node2VecRandomWalkTask> tasks(
        CompressedRandomWalks compressedRandomWalks,
        RandomWalkProbabilities.Builder randomWalkPropabilitiesBuilder,
        Graph graph,
        Optional<Long> maybeRandomSeed,
        int concurrency,
        List<Long> sourceNodes,
        WalkParameters walkParameters,
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
                randomSeed,
                walkParameters.walkLength,
                walkParameters.returnFactor,
                walkParameters.inOutFactor
            ));
        }
        return tasks;
    }
}
