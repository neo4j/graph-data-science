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
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.BitUtil;

import java.util.concurrent.CountedCompleter;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public final class ForkJoinComputeStep<CONFIG extends PregelConfig, ITERATOR extends Messages.MessageIterator, COMPUTE_CONTEXT extends ComputeContext<CONFIG>>
    extends CountedCompleter<Void>
    implements ComputeStep<CONFIG, ITERATOR, COMPUTE_CONTEXT> {

    private static final int SEQUENTIAL_THRESHOLD = 1000;

    private final ComputeFunction<CONFIG, COMPUTE_CONTEXT> computeFunction;
    private final CONFIG config;

    private final InitContext<CONFIG> initContext;
    private final Supplier<COMPUTE_CONTEXT> computeContext;
    private final NodeValue nodeValue;
    private final HugeAtomicBitSet voteBits;
    private final Messenger<ITERATOR> messenger;
    private final BasePregelComputation<CONFIG> computation;

    private Partition nodeBatch;
    private final MutableInt iteration;
    private final AtomicBoolean hasSentMessage;
    private final ProgressTracker progressTracker;

    ForkJoinComputeStep(
        BasePregelComputation<CONFIG> computation,
        ComputeFunction<CONFIG, COMPUTE_CONTEXT> computeFunction,
        Supplier<COMPUTE_CONTEXT> computeContext,
        InitContext<CONFIG> initContext,
        CONFIG config,
        MutableInt iteration,
        Partition nodeBatch,
        NodeValue nodeValue,
        Messenger<ITERATOR> messenger,
        HugeAtomicBitSet voteBits,
        @Nullable CountedCompleter<Void> parent,
        AtomicBoolean sentMessage,
        ProgressTracker progressTracker
    ) {
        super(parent);
        this.computeFunction = computeFunction;
        this.config = config;
        this.iteration = iteration;
        this.computation = computation;
        this.voteBits = voteBits;
        this.nodeBatch = nodeBatch;
        this.nodeValue = nodeValue;
        this.messenger = messenger;
        this.computeContext = computeContext;
        this.hasSentMessage = sentMessage;
        this.progressTracker = progressTracker;
        this.initContext = initContext;
    }

    @Override
    public void compute() {
        if (nodeBatch.nodeCount() >= SEQUENTIAL_THRESHOLD) {
            long startNode = nodeBatch.startNode();
            long batchSize = nodeBatch.nodeCount();
            boolean isEven = batchSize % 2 == 0;

            long pivot = BitUtil.ceilDiv(batchSize, 2);

            var rightBatch = isEven
                ? Partition.of(startNode + pivot, pivot)
                : Partition.of(startNode + pivot, pivot - 1);

            var leftBatch = Partition.of(startNode, pivot);

            var leftTask = new ForkJoinComputeStep<>(
                computation,
                computeFunction,
                computeContext,
                initContext,
                config,
                iteration,
                leftBatch,
                nodeValue,
                messenger,
                voteBits,
                this,
                hasSentMessage,
                progressTracker
            );

            this.nodeBatch = rightBatch;

            addToPendingCount(1);
            leftTask.fork();

            this.compute();
        } else {
            computeBatch();
            tryComplete();
        }
    }

    @Override
    public HugeAtomicBitSet voteBits() {
        return voteBits;
    }

    @Override
    public BasePregelComputation<CONFIG> computation() {
        return computation;
    }

    @Override
    public ComputeFunction<CONFIG, COMPUTE_CONTEXT> computeFunction() {
        return computeFunction;
    }

    @Override
    public NodeValue nodeValue() {
        return nodeValue;
    }

    @Override
    public Messenger<ITERATOR> messenger() {
        return messenger;
    }

    @Override
    public Partition nodeBatch() {
        return nodeBatch;
    }

    @Override
    public InitContext<CONFIG> initContext() {
        return initContext;
    }

    @Override
    public COMPUTE_CONTEXT computeContext() {
        return computeContext.get();
    }

    @Override
    public ProgressTracker progressTracker() {
        return progressTracker;
    }
}
