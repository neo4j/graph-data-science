/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.impl.closeness;

import org.neo4j.graphalgo.LegacyAlgorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicIntegerArray;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Normalized Closeness Centrality
 *
 * Utilizes the MSBFS for counting the farness between nodes.
 * See MSBFS documentation.
 *
 *
 *
 * @author mknblch
 */
public class MSClosenessCentrality extends LegacyAlgorithm<MSClosenessCentrality> {

    private Graph graph;
    private PagedAtomicIntegerArray farness;
    private PagedAtomicIntegerArray component;

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

    public MSClosenessCentrality compute(Direction direction) {

        final ProgressLogger progressLogger = getProgressLogger();

        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.add(nodeId, len * depth);
            while (sourceNodeIds.hasNext()) {
                component.add(sourceNodeIds.next(), 1);
            }
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };

        new MultiSourceBFS(
                graph,
                graph,
                direction,
                consumer,
                tracker)
                .run(concurrency, executorService);

        return this;
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
        exporter.write(
                propertyName,
                farness,
                (PropertyTranslator.OfDouble<PagedAtomicIntegerArray>)
                        (data, nodeId) -> centrality(data.get(nodeId), component.get(nodeId), nodeCount, wassermanFaust));
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
    public void release() {
        graph = null;
        farness = null;
    }

    @Override
    public Void compute() {
        compute(Direction.OUTGOING);
        return null;
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
            return (componentSize / ((double) farness)) * ((componentSize - 1.) / (nodeCount - 1.));
        }
        return componentSize / ((double) farness);
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
