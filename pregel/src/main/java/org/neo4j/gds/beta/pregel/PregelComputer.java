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

import org.immutables.builder.Builder;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

abstract class PregelComputer<CONFIG extends PregelConfig> {
    final Graph graph;
    final BasePregelComputation<CONFIG> computation;
    final CONFIG config;
    final NodeValue nodeValues;
    final Messenger<?> messenger;
    final HugeAtomicBitSet voteBits;
    final ProgressTracker progressTracker;

    PregelComputer(
        Graph graph,
        BasePregelComputation<CONFIG> computation,
        CONFIG config,
        NodeValue nodeValues,
        Messenger<?> messenger,
        HugeAtomicBitSet voteBits,
        ProgressTracker progressTracker
    ) {
        this.graph = graph;
        this.computation = computation;
        this.config = config;
        this.nodeValues = nodeValues;
        this.messenger = messenger;
        this.voteBits = voteBits;
        this.progressTracker = progressTracker;
    }

    abstract void initComputation();

    abstract void initIteration(int iteration);

    abstract void runIteration();

    abstract boolean hasConverged();

    abstract void release();

    static <CONFIG extends PregelConfig> ComputerBuilder<CONFIG> builder() {
        return new ComputerBuilder<>();
    }

    @Builder.Factory
    static <CONFIG extends PregelConfig> PregelComputer<CONFIG> computer(
        Graph graph,
        BasePregelComputation<CONFIG> computation,
        CONFIG config,
        NodeValue nodeValues,
        Messenger<?> messenger,
        HugeAtomicBitSet voteBits,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        if (config.useForkJoin()) {
            if (!(executorService instanceof ForkJoinPool)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "Required ForkJoinPool, got %s",
                    executorService.getClass()
                ));
            }

            return new ForkJoinComputer<>(
                graph,
                computation,
                config,
                nodeValues,
                messenger,
                voteBits,
                (ForkJoinPool) executorService,
                progressTracker
            );
        }

        return new PartitionedComputer<>(
            graph,
            computation,
            config,
            nodeValues,
            messenger,
            voteBits,
            config.concurrency(),
            executorService,
            progressTracker
        );
    }
}
