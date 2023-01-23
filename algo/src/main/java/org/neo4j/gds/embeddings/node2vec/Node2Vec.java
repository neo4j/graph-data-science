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
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.traversal.RandomWalk;

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

        RandomWalk randomWalk = RandomWalk.create(
            graph,
            config,
            progressTracker,
            Pools.DEFAULT
        );

        var probabilitiesBuilder = new RandomWalkProbabilities.Builder(
            graph.nodeCount(),
            config.positiveSamplingFactor(),
            config.negativeSamplingExponent(),
            config.concurrency()
        );
        var walks = new CompressedRandomWalks(graph.nodeCount() * config.walksPerNode());

        randomWalk.compute().forEach(walk -> {
            probabilitiesBuilder.registerWalk(walk);
            walks.add(walk);
        });

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

}
