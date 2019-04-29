/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.write.Exporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.function.LongToIntFunction;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Normalized Closeness Centrality.
 *
 * Utilizes the MSBFS for counting the farness between nodes.
 * See MSBFS documentation.
 *
 *
 *
 * @author mknblch
 */
public class MSClosenessCentrality extends MSBFSCCAlgorithm<MSClosenessCentrality> {

    private Graph graph;
    private AtomicIntegerArray farness;
    private AtomicIntegerArray component;

    private final int concurrency;
    private final ExecutorService executorService;
    private final int nodeCount;

    private final boolean wassermanFaust;

    public MSClosenessCentrality(Graph graph, int concurrency, ExecutorService executorService, boolean wassermanFaust) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.wassermanFaust = wassermanFaust;
        farness = new AtomicIntegerArray(nodeCount);
        component = new AtomicIntegerArray(nodeCount);
    }

    @Override
    public MSClosenessCentrality compute(Direction direction) {

        final ProgressLogger progressLogger = getProgressLogger();
        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            // number of source node IDs
            int len = sourceNodeIds.size();
            // sum of distances
            farness.addAndGet(nodeId, len * depth);
            // count component size too
            while (sourceNodeIds.hasNext()) {
                component.incrementAndGet(sourceNodeIds.next());
            }
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };
        new MultiSourceBFS(graph, graph, direction, consumer)
                .run(concurrency, executorService);
        return this;
    }

    @Override
    public double[] getCentrality() {
        final double[] cc = new double[nodeCount];
        Arrays.parallelSetAll(cc, i -> centrality(farness.get(i),
                component.get(i),
                nodeCount,
                wassermanFaust));
        return cc;
    }

    @Override
    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(nodeId -> new Result(
                        graph.toOriginalNodeId(nodeId),
                        centrality(farness.get(nodeId), component.get(nodeId), nodeCount, wassermanFaust)));
    }

    @Override
    public void export(final String propertyName, final Exporter exporter) {
        exporter.write(
                propertyName,
                farness,
                (PropertyTranslator.OfDouble<AtomicIntegerArray>)
                        (data, nodeId) ->
                                centrality(farness.get((int) nodeId), component.get((int) nodeId), nodeCount, wassermanFaust));
    }

    @Override
    public MSClosenessCentrality me() {
        return this;
    }

    @Override
    public MSClosenessCentrality release() {
        graph = null;
        farness = null;
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
