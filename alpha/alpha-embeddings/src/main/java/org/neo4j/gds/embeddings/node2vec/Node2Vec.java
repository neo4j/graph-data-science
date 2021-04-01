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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;

public class Node2Vec extends Algorithm<Node2Vec, HugeObjectArray<Vector>> {

    private final Graph graph;
    private final Node2VecBaseConfig config;
    private final AllocationTracker tracker;

    public static MemoryEstimation memoryEstimation(Node2VecBaseConfig config) {
        return MemoryEstimations.builder(Node2Vec.class)
            .perNode("random walks", (nodeCount) -> {
                var numberOfRandomWalks = nodeCount * config.walksPerNode();
                var randomWalkMemoryUsage = MemoryUsage.sizeOfLongArray(config.walkLength());
                return HugeObjectArray.memoryEstimation(numberOfRandomWalks, randomWalkMemoryUsage);
            })
            .add("probability cache", ProbabilityComputer.memoryEstimation())
            .add("model", Node2VecModel.memoryEstimation(config))
            .build();
    }

    public Node2Vec(Graph graph, Node2VecBaseConfig config, ProgressLogger progressLogger, AllocationTracker tracker) {
        this.graph = graph;
        this.config = config;
        this.progressLogger = progressLogger;
        this.tracker = tracker;
    }

    @Override
    public HugeObjectArray<Vector> compute() {
        RandomWalk randomWalk = new RandomWalk(
            graph,
            config.walkLength(),
            config.concurrency(),
            config.walksPerNode(),
            config.walkBufferSize(),
            config.returnFactor(),
            config.inOutFactor()
        );

        HugeObjectArray<long[]> walks = HugeObjectArray.newArray(
            long[].class,
            graph.nodeCount() * config.walksPerNode(),
            tracker
        );
        MutableLong counter = new MutableLong(0);
        randomWalk
            .compute()
            .forEach(walk -> {
                walks.set(counter.longValue(), walk);
                counter.increment();
            });

        var probabilityComputer = new ProbabilityComputer(
            walks,
            graph.nodeCount(),
            config.centerSamplingFactor(),
            config.contextSamplingExponent(),
            config.concurrency(),
            tracker
        );

        var node2VecModel = new Node2VecModel(
            graph.nodeCount(),
            config,
            walks,
            probabilityComputer,
            progressLogger,
            tracker
        );

        node2VecModel.train();

        return node2VecModel.getEmbeddings();
    }

    @Override
    public Node2Vec me() {
        return this;
    }

    @Override
    public void release() {

    }
}
