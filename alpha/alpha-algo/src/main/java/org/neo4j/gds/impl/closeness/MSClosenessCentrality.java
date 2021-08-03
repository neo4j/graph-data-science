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
import org.neo4j.gds.impl.msbfs.BfsConsumer;
import org.neo4j.gds.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.nodeproperties.DoubleNodeProperties;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Normalized Closeness Centrality
 *
 * Utilizes the MSBFS for counting the farness between nodes.
 * See MSBFS documentation.
 */
public class MSClosenessCentrality extends Algorithm<MSClosenessCentrality, MSClosenessCentrality> {

    private final Graph graph;
    private final PagedAtomicIntegerArray farness;
    private final PagedAtomicIntegerArray component;

    private final int concurrency;
    private final ExecutorService executorService;
    private final long nodeCount;
    private final AllocationTracker tracker;

    private final boolean wassermanFaust;

    public MSClosenessCentrality(
            Graph graph,
            AllocationTracker tracker,
            int concurrency,
            ExecutorService executorService, boolean wassermanFaust) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.tracker = tracker;
        this.wassermanFaust = wassermanFaust;
        farness = PagedAtomicIntegerArray.newArray(nodeCount, this.tracker);
        component = PagedAtomicIntegerArray.newArray(nodeCount, this.tracker);
    }

    public HugeDoubleArray getCentrality() {
        final HugeDoubleArray cc = HugeDoubleArray.newArray(nodeCount, tracker);
        for (int i = 0; i < nodeCount; i++) {
            cc.set(i, centrality(farness.get(i),
                    component.get(i),
                    nodeCount,
                    wassermanFaust));
        }
        return cc;
    }

    public void export(final String propertyName, final NodePropertyExporter exporter) {
        DoubleNodeProperties properties = new DoubleNodeProperties() {
            @Override
            public double doubleValue(long nodeId) {
                return centrality(
                    farness.get(nodeId),
                    component.get(nodeId),
                    nodeCount,
                    wassermanFaust
                );
            }

            @Override
            public long size() {
                return graph.nodeCount();
            }
        };

        exporter.write(
            propertyName,
            properties
        );
    }

    public Stream<MSClosenessCentrality.Result> resultStream() {
        return LongStream.range(0L, nodeCount)
                .mapToObj(nodeId -> new MSClosenessCentrality.Result(
                        graph.toOriginalNodeId(nodeId),
                        centrality(farness.get(nodeId), component.get(nodeId), nodeCount, wassermanFaust)
                ));
    }

    @Override
    public MSClosenessCentrality me() {
        return this;
    }

    @Override
    public void release() {}

    @Override
    public MSClosenessCentrality compute() {
        progressTracker.beginSubTask();
        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.add(nodeId, len * depth);
            while (sourceNodeIds.hasNext()) {
                component.add(sourceNodeIds.next(), 1);
            }
            progressTracker.logProgress();
        };

        MultiSourceBFS
            .aggregatedNeighborProcessing(graph, graph, consumer, tracker)
            .run(concurrency, executorService);

        progressTracker.endSubTask();
        return this;
    }

    public final double[] exportToArray() {
        return resultStream()
                .limit(Integer.MAX_VALUE)
                .mapToDouble(r -> r.centrality)
                .toArray();
    }

    static double centrality(long farness, long componentSize, long nodeCount, boolean wassermanFaust) {
        if (farness == 0L) {
            return 0.;
        }
        if (wassermanFaust) {
            return (componentSize / ((double) farness)) * ((componentSize) / (nodeCount - 1.));
        } else {
            return componentSize / ((double) farness);
        }
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
