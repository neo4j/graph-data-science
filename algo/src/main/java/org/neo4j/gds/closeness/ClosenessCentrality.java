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
package org.neo4j.gds.closeness;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.haa.HugeAtomicIntArray;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.paged.ParallelIntPageCreator;
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

    public static final String CLOSENESS_DESCRIPTION =
        "Closeness centrality is a way of detecting nodes that are " +
        "able to spread information very efficiently through a graph.";

    private final Graph graph;
    private final long nodeCount;
    private final int concurrency;
    private final ExecutorService executorService;
    private final HugeAtomicIntArray farness;
    private final HugeAtomicIntArray component;
    private final CentralityComputer centralityComputer;

    ClosenessCentrality(
        Graph graph,
        int concurrency,
        CentralityComputer centralityComputer,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);
        this.graph = graph;
        this.nodeCount = graph.nodeCount();
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.centralityComputer = centralityComputer;
        this.farness = HugeAtomicIntArray.of(nodeCount, ParallelIntPageCreator.of(concurrency));
        this.component = HugeAtomicIntArray.of(nodeCount, ParallelIntPageCreator.of(concurrency));
    }

    @Override
    public ClosenessCentralityResult compute() {
        progressTracker.beginSubTask();
        computeFarness();
        var centralities = computeCloseness();
        progressTracker.endSubTask();

        return new ClosenessCentralityResult(centralities);
    }

    private void computeFarness() {
        progressTracker.beginSubTask();
        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.getAndAdd(nodeId, len * depth);
            component.getAndAdd(nodeId, len);
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
