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
package org.neo4j.graphalgo.beta.pregel;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.graphalgo.core.utils.partition.Partition;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;

public class ForkJoinComputer<CONFIG extends PregelConfig> implements PregelComputer {

    private final Graph graph;
    private final PregelComputation<CONFIG> computation;
    private final CONFIG config;
    private final NodeValue nodeValues;
    private final Messenger<?> messenger;
    private final HugeAtomicBitSet voteBits;
    private final ForkJoinPool forkJoinPool;

    private AtomicBoolean sentMessage;
    private ComputeStepFJ<CONFIG, ?> rootTask;

    ForkJoinComputer(
        Graph graph,
        PregelComputation<CONFIG> computation,
        CONFIG config,
        NodeValue nodeValues,
        Messenger<?> messenger,
        HugeAtomicBitSet voteBits,
        ForkJoinPool forkJoinPool
    ) {
        this.graph = graph;
        this.computation = computation;
        this.config = config;
        this.nodeValues = nodeValues;
        this.messenger = messenger;
        this.voteBits = voteBits;
        this.forkJoinPool = forkJoinPool;
    }

    @Override
    public void initComputation() {
        // silence is golden
    }

    @Override
    public void initIteration(int iteration) {
        this.sentMessage = new AtomicBoolean(false);
        this.rootTask = new ComputeStepFJ<>(
            graph,
            computation,
            config,
            iteration,
            Partition.of(0, graph.nodeCount()),
            nodeValues,
            messenger,
            voteBits,
            graph,
            null,
            sentMessage
        );
    }

    @Override
    public void runIteration() {
        forkJoinPool.invoke(rootTask);
    }

    @Override
    public boolean hasConverged() {
        return !sentMessage.get() && voteBits.allSet();
    }
}
