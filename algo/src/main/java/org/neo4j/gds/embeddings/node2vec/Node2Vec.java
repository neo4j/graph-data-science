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
import org.neo4j.gds.traversal.RandomWalkBaseConfig;
import org.neo4j.gds.traversal.RandomWalkCompanion;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class Node2Vec extends Algorithm<Node2VecModel.Result> {

    private final Graph graph;
    private final Node2VecBaseConfig config;

    public static MemoryEstimation memoryEstimation(Node2VecBaseConfig config) {
        return MemoryEstimations.builder(Node2Vec.class.getSimpleName())
            .perNode("random walks", (nodeCount) -> {
                var numberOfRandomWalks = nodeCount * config.walksPerNode();
                var randomWalkMemoryUsage = MemoryUsage.sizeOfLongArray(config.walkLength());
                return HugeObjectArray.memoryEstimation(numberOfRandomWalks, randomWalkMemoryUsage);
            })
            .add("probability cache", RandomWalkProbabilities.memoryEstimation())
            .add("model", Node2VecModel.memoryEstimation(config))
            .build();
    }

    public Node2Vec(Graph graph, Node2VecBaseConfig config, ProgressTracker progressTracker) {
        super(progressTracker);
        this.graph = graph;
        this.config = config;
    }

    @Override
    public Node2VecModel.Result compute() {
        progressTracker.beginSubTask("Node2Vec");

        if (graph.hasRelationshipProperty()) {
            EmbeddingUtils.validateRelationshipWeightPropertyValue(
                graph,
                config.concurrency(),
                weight -> weight >= 0,
                "Node2Vec only supports non-negative weights.",
                DefaultPool.INSTANCE
            );
        }

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            graph.nodeCount(),
            config.positiveSamplingFactor(),
            config.negativeSamplingExponent(),
            config.concurrency()
        );
        var walks = new CompressedRandomWalks(graph.nodeCount() * config.walksPerNode());

        progressTracker.beginSubTask("RandomWalk");

        var tasks = tasks(
            walks,
            probabilitiesBuilder,
            graph,
            config,
            DefaultPool.INSTANCE,
            progressTracker,
            terminationFlag
        );

        progressTracker.beginSubTask("create walks");
        RunWithConcurrency.builder().concurrency(config.concurrency()).tasks(tasks).run();
        walks.setMaxWalkLength(tasks.stream()
            .map(task -> task.maxWalkLength())
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
            config,
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
        RandomWalkBaseConfig config,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        ArrayList<Node2VecRandomWalkTask> tasks = new ArrayList<>();
        var randomSeed = config.randomSeed().orElseGet(() -> new Random().nextLong());
        int concurrency = config.concurrency();
        var nextNodeSupplier = RandomWalkCompanion.nextNodeSupplier(graph, config);
        var cumulativeWeightsSupplier = RandomWalkCompanion.cumulativeWeights(
            graph,
            config,
            executorService,
            progressTracker
        );

        AtomicLong index = new AtomicLong();
        for (int i = 0; i < concurrency; ++i) {
            tasks.add(new Node2VecRandomWalkTask(nextNodeSupplier,
                cumulativeWeightsSupplier,
                config,
                graph.concurrentCopy(),
                randomSeed,
                progressTracker,
                terminationFlag,
                index,
                compressedRandomWalks,
                randomWalkPropabilitiesBuilder
            ));
        }
        return tasks;
    }

}
