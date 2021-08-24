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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.beta.pregel.context.InitContext;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

public final class PartitionedComputeStep<CONFIG extends PregelConfig, ITERATOR extends Messages.MessageIterator>
    implements Runnable, ComputeStep<CONFIG, ITERATOR> {

    private final InitContext<CONFIG> initContext;
    private final ComputeContext<CONFIG> computeContext;
    private final ProgressTracker progressTracker;
    private final Partition nodeBatch;
    private final HugeAtomicBitSet voteBits;
    private final Messenger<ITERATOR> messenger;
    private final PregelComputation<CONFIG> computation;

    private final Graph graph;
    private int iteration;
    private final NodeValue nodeValue;
    private boolean hasSentMessage;

    PartitionedComputeStep(
        Graph graph,
        PregelComputation<CONFIG> computation,
        CONFIG config,
        int iteration,
        Partition nodeBatch,
        NodeValue nodeValue,
        Messenger<ITERATOR> messenger,
        HugeAtomicBitSet voteBits,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.iteration = iteration;
        this.nodeValue = nodeValue;
        this.computation = computation;
        this.voteBits = voteBits;
        this.nodeBatch = nodeBatch;
        this.messenger = messenger;
        this.computeContext = new ComputeContext<>(this, config);
        this.progressTracker = progressTracker;
        this.initContext = new InitContext<>(this, config, graph);
    }

    @Override
    public void run() {
        computeBatch();
    }

    @Override
    public Graph graph() {
        return graph;
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

    @Override
    public int iteration() {
        return iteration;
    }

    @Override
    public void sendTo(long targetNodeId, double message) {
        messenger.sendTo(targetNodeId, message);
        hasSentMessage = true;
    }

    void init(int iteration) {
        this.iteration = iteration;
        this.hasSentMessage = false;
    }

    boolean hasSentMessage() {
        return hasSentMessage;
    }
}
