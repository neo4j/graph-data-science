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

public final class PartitionedComputeStep<CONFIG extends PregelConfig, ITERATOR extends Messages.MessageIterator>
    implements Runnable, ComputeStep<CONFIG, ITERATOR> {

    private final InitContext<CONFIG> initContext;
    private final ComputeContext<CONFIG> computeContext;
    private final ProgressTracker progressTracker;
    private final Partition nodeBatch;
    private final HugeAtomicBitSet voteBits;
    private final Messenger<ITERATOR> messenger;
    private final PregelComputation<CONFIG> computation;

    private final MutableInt iteration;
    private final AtomicBoolean hasSentMessage;
    private final NodeValue nodeValue;

    PartitionedComputeStep(
        Graph graph,
        PregelComputation<CONFIG> computation,
        CONFIG config,
        Partition nodeBatch,
        NodeValue nodeValue,
        Messenger<ITERATOR> messenger,
        HugeAtomicBitSet voteBits,
        ProgressTracker progressTracker
    ) {
        this.nodeValue = nodeValue;
        this.computation = computation;
        this.voteBits = voteBits;
        this.nodeBatch = nodeBatch;
        this.messenger = messenger;
        this.progressTracker = progressTracker;
        this.iteration = new MutableInt(0);
        this.hasSentMessage = new AtomicBoolean(false);
        this.computeContext = new ComputeContext<>(graph, computation, config, nodeValue, messenger, voteBits, iteration, hasSentMessage, progressTracker);
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
    public PregelComputation<CONFIG> computation() {
        return computation;
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
    public ComputeContext<CONFIG> computeContext() {
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
