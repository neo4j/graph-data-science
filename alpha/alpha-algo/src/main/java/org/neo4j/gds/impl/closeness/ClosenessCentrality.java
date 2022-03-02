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
package org.neo4j.gds.impl.closeness;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.msbfs.BfsConsumer;
import org.neo4j.gds.msbfs.MultiSourceBFS;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Normalized Closeness Centrality
 *
 * Utilizes the MSBFS for counting the farness between nodes.
 * See MSBFS documentation.
 */
public class ClosenessCentrality extends Algorithm<ClosenessCentrality> {

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

    public ClosenessCentrality(
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

    public HugeDoubleArray getCentrality() {
        final HugeDoubleArray cc = HugeDoubleArray.newArray(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            cc.set(i, centrality(
                farness.get(i),
                component.get(i),
                nodeCount,
                wassermanFaust
            ));
        }
        return cc;
    }

    public Stream<ClosenessCentrality.Result> resultStream() {
        return LongStream.range(0L, nodeCount)
            .mapToObj(nodeId -> new ClosenessCentrality.Result(
                graph.toOriginalNodeId(nodeId),
                centrality(farness.get(nodeId), component.get(nodeId), nodeCount, wassermanFaust)
            ));
    }

    @Override
    public void release() {}

    @Override
    public ClosenessCentrality compute() {
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
        return this;
    }

    /**
     * Result class used for streaming
     */
    public static final class Result {

        public final long nodeId;

        public final double centrality;

        public Result(long nodeId, double centrality) {
            this.nodeId = nodeId;
            this.centrality = centrality;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", centrality=" + centrality +
                    '}';
        }
    }
}
