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
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.msbfs.BfsConsumer;
import org.neo4j.gds.msbfs.MultiSourceBFSAccessMethods;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * Normalized Closeness Centrality
 *
 * Utilizes the MSBFS for counting the farness between nodes.
 * See MSBFS documentation.
 */
public final class ClosenessCentrality extends Algorithm<ClosenessCentralityResult> {

    private final Graph graph;
    private final long nodeCount;
    private final int concurrency;
    private final ExecutorService executorService;
    private final PagedAtomicIntegerArray farness;
    private final PagedAtomicIntegerArray component;
    private final CentralityComputer centralityComputer;

    public static ClosenessCentrality of(
        Graph graph,
        ClosenessCentralityConfig config,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        var nodeCount = graph.nodeCount();
        var centralityComputer = config.useWassermanFaust()
            ? new WassermanFaustCentralityComputer(nodeCount)
            : new DefaultCentralityComputer();
        return new ClosenessCentrality(
            graph,
            nodeCount,
            config.concurrency(),
            centralityComputer,
            PagedAtomicIntegerArray.newArray(nodeCount),
            PagedAtomicIntegerArray.newArray(nodeCount),
            executorService,
            progressTracker
        );
    }

    private ClosenessCentrality(
        Graph graph,
        long nodeCount,
        int concurrency,
        CentralityComputer centralityComputer,
        PagedAtomicIntegerArray farness,
        PagedAtomicIntegerArray component,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = nodeCount;
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.centralityComputer = centralityComputer;
        this.farness = farness;
        this.component = component;
    }

    @Override
    public ClosenessCentralityResult compute() {
        progressTracker.beginSubTask();
        computeFarness();
        var centralities = computeCloseness();
        progressTracker.endSubTask();

        return ImmutableClosenessCentralityResult.of(centralities);
    }

    private void computeFarness() {
        progressTracker.beginSubTask();
        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.add(nodeId, len * depth);
            component.add(nodeId, len);
            progressTracker.logProgress();
        };
        MultiSourceBFSAccessMethods
            .aggregatedNeighborProcessingWithoutSourceNodes(nodeCount, graph, consumer)
            .run(concurrency, executorService);
        progressTracker.endSubTask();
    }

    private HugeDoubleArray computeCloseness() {
        progressTracker.beginSubTask();

        var closeness = HugeDoubleArray.newArray(nodeCount);

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            nodeCount,
            partition -> (Runnable) () -> {
                partition.consume(nodeId -> closeness.set(nodeId, centralityComputer.centrality(
                    farness.get(nodeId),
                    component.get(nodeId)
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
