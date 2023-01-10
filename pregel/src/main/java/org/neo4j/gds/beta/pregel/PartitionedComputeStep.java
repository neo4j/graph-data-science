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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.concurrent.atomic.AtomicBoolean;

public final class PartitionedComputeStep<CONFIG extends PregelConfig, ITERATOR extends Messages.MessageIterator, COMPUTE_CONTEXT extends ComputeContext<CONFIG>>
    implements Runnable, ComputeStep<CONFIG, ITERATOR, COMPUTE_CONTEXT> {

    private final InitContext<CONFIG> initContext;
    private final COMPUTE_CONTEXT computeContext;
    private final ProgressTracker progressTracker;
    private final Partition nodeBatch;
    private final HugeAtomicBitSet voteBits;
    private final Messenger<ITERATOR> messenger;
    private final BasePregelComputation<CONFIG> computation;

    private final MutableInt iteration;
    private final AtomicBoolean hasSentMessage;
    private final ComputeFunction<CONFIG, COMPUTE_CONTEXT> computeFunction;
    private final NodeValue nodeValue;

    PartitionedComputeStep(
        Graph graph,
        BasePregelComputation<CONFIG> computation,
        ComputeFunction<CONFIG, COMPUTE_CONTEXT> computeFunction,
        COMPUTE_CONTEXT computeContext,
        CONFIG config,
        Partition nodeBatch,
        NodeValue nodeValue,
        Messenger<ITERATOR> messenger,
        HugeAtomicBitSet voteBits,
        MutableInt iteration,
        AtomicBoolean hasSentMessage,
        ProgressTracker progressTracker
    ) {
        this.computeFunction = computeFunction;
        this.nodeValue = nodeValue;
        this.computation = computation;
        this.voteBits = voteBits;
        this.nodeBatch = nodeBatch;
        this.messenger = messenger;
        this.progressTracker = progressTracker;
        this.iteration = iteration;
        this.hasSentMessage = hasSentMessage;
        this.computeContext = computeContext;
        this.initContext = new InitContext<>(graph, config, nodeValue, progressTracker);
    }

    @Override
    public void run() {
        computeBatch();
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
        return computeContext;
    }

    @Override
    public ProgressTracker progressTracker() {
        return progressTracker;
    }

    void init(int iteration) {
        this.iteration.setValue(iteration);
        this.hasSentMessage.set(false);
    }

    boolean hasSentMessage() {
        return hasSentMessage.get();
    }
}
