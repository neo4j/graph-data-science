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
package org.neo4j.gds.beta.pregel;

import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.ComputeContext.BidirectionalComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.beta.pregel.context.InitContext.BidirectionalInitContext;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class ForkJoinComputer<CONFIG extends PregelConfig> extends PregelComputer<CONFIG> {

    private final ForkJoinPool forkJoinPool;

    private AtomicBoolean sentMessage;
    private ForkJoinComputeStep<CONFIG, ?, ?, ?> rootTask;

    ForkJoinComputer(
        Graph graph,
        BasePregelComputation<CONFIG> computation,
        CONFIG config,
        NodeValue nodeValues,
        Messenger<?> messenger,
        HugeAtomicBitSet voteBits,
        ForkJoinPool forkJoinPool,
        ProgressTracker progressTracker
    ) {
        super(graph, computation, config, nodeValues, messenger, voteBits, progressTracker);
        this.forkJoinPool = forkJoinPool;
    }

    @Override
    public void initComputation() {
        // silence is golden
    }

    @Override
    public void initIteration(int iteration) {
        this.sentMessage = new AtomicBoolean(false);
        MutableInt mutableIteration = new MutableInt(iteration);
        Partition partition = Partition.of(0, graph.nodeCount());

        this.rootTask = computation instanceof PregelComputation<CONFIG>
            ? createComputeStep(mutableIteration, sentMessage, partition)
            : createBidirectionalComputeSteps(mutableIteration, sentMessage, partition);
    }

    @Override
    public void runIteration() {
        forkJoinPool.invoke(rootTask);
    }

    @Override
    public boolean hasConverged() {
        return !sentMessage.get() && voteBits.allSet();
    }

    @Override
    void release() {
        forkJoinPool.shutdown();
        computation.close();
    }

    @NotNull
    private ForkJoinComputeStep<CONFIG, ?, InitContext<CONFIG>, ComputeContext<CONFIG>> createComputeStep(
        MutableInt iteration,
        AtomicBoolean hasSentMessages,
        Partition partition
    ) {
        Supplier<InitContext<CONFIG>> initContext = () -> new InitContext<>(
            graph.concurrentCopy(),
            config,
            nodeValues,
            progressTracker
        );

        Supplier<ComputeContext<CONFIG>> computeContext = () -> new ComputeContext<>(
            graph.concurrentCopy(),
            config,
            ((PregelComputation<CONFIG>) computation)::applyRelationshipWeight,
            nodeValues,
            messenger,
            voteBits,
            iteration,
            hasSentMessages,
            progressTracker
        );

        return new ForkJoinComputeStep<>(
            ((PregelComputation<CONFIG>) computation)::init,
            ((PregelComputation<CONFIG>) computation)::compute,
            initContext,
            computeContext,
            iteration,
            partition,
            nodeValues,
            messenger,
            voteBits,
            null,
            hasSentMessages,
            progressTracker
        );
    }

    @NotNull
    private ForkJoinComputeStep<CONFIG, ?, BidirectionalInitContext<CONFIG>, BidirectionalComputeContext<CONFIG>> createBidirectionalComputeSteps(
        MutableInt iteration,
        AtomicBoolean hasSentMessages,
        Partition partition
    ) {
        Supplier<BidirectionalInitContext<CONFIG>> initContext = () -> new BidirectionalInitContext<>(
            graph.concurrentCopy(),
            config,
            nodeValues,
            progressTracker
        );

        Supplier<BidirectionalComputeContext<CONFIG>> computeContext = () -> new BidirectionalComputeContext<>(
            graph.concurrentCopy(),
            config,
            ((BidirectionalPregelComputation<CONFIG>) computation)::applyRelationshipWeight,
            nodeValues,
            messenger,
            voteBits,
            iteration,
            hasSentMessages,
            progressTracker
        );

        return new ForkJoinComputeStep<>(
            ((BidirectionalPregelComputation<CONFIG>) computation)::init,
            ((BidirectionalPregelComputation<CONFIG>) computation)::compute,
            initContext,
            computeContext,
            iteration,
            partition,
            nodeValues,
            messenger,
            voteBits,
            null,
            hasSentMessages,
            progressTracker
        );
    }
}
