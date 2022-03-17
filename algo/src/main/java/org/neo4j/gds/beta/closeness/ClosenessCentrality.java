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
package org.neo4j.gds.beta.closeness;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.HugeAtomicDoubleArray;
import org.neo4j.gds.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.msbfs.BfsConsumer;
import org.neo4j.gds.msbfs.MultiSourceBFS;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Normalized Closeness Centrality
 *
 * Utilizes the MSBFS for counting the farness between nodes.
 * See MSBFS documentation.
 */
public final class ClosenessCentrality extends Algorithm<ClosenessCentralityResult> {

    static double centrality(long farness, long componentSize, long nodeCount, boolean wassermanFaust) {
        if (farness == 0L) {
            return 0.0D;
        }
        if (wassermanFaust) {
            return (componentSize / ((double) farness)) * ((componentSize) / (nodeCount - 1.0D));
        }
        return componentSize / ((double) farness);
    }

    private final Graph graph;
    private final long nodeCount;
    private final int concurrency;
    private final ExecutorService executorService;
    private final boolean wassermanFaust;
    private final PagedAtomicIntegerArray farness;
    private final PagedAtomicIntegerArray component;

    public static ClosenessCentrality of(
        Graph graph,
        ClosenessCentralityConfig config,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        return new ClosenessCentrality(
            graph,
            config.concurrency(),
            config.useWassermanFaust(),
            executorService,
            progressTracker
        );
    }

    private ClosenessCentrality(
        Graph graph,
        int concurrency,
        boolean wassermanFaust,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.wassermanFaust = wassermanFaust;
        this.farness = PagedAtomicIntegerArray.newArray(nodeCount);
        this.component = PagedAtomicIntegerArray.newArray(nodeCount);
    }

    @Override
    public ClosenessCentralityResult compute() {
        progressTracker.beginSubTask();
        computeFarness();
        var centralities = computeCloseness();
        progressTracker.endSubTask();

        return ImmutableClosenessCentralityResult.of(centralities);
    }

    @Override
    public void release() {}

    private void computeFarness() {
        progressTracker.beginSubTask();
        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.add(nodeId, len * depth);
            component.add(nodeId, len);
            progressTracker.logProgress();
        };
        MultiSourceBFS
            .aggregatedNeighborProcessing(graph.nodeCount(), graph, consumer)
            .run(concurrency, executorService);
        progressTracker.endSubTask();
    }

    private HugeAtomicDoubleArray computeCloseness() {
        progressTracker.beginSubTask();

        var closeness = HugeAtomicDoubleArray.newArray(nodeCount);

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> (Runnable) () -> {
                partition.consume(nodeId -> closeness.set(nodeId, centrality(
                    farness.get(nodeId),
                    component.get(nodeId),
                    nodeCount,
                    wassermanFaust
                )));
                progressTracker.logProgress(partition.nodeCount());
            },
            Optional.empty()
        );

        ParallelUtil.run(tasks, executorService);

        progressTracker.endSubTask();
        
        return closeness;
    }
}
