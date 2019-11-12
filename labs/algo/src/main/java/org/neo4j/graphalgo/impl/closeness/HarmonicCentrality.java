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

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.PagedAtomicDoubleArray;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * Harmonic Centrality Algorithm
 *
 * @author mknblch
 */
public class HarmonicCentrality extends Algorithm<HarmonicCentrality> {

    private Graph graph;
    private final AllocationTracker allocationTracker;
    private PagedAtomicDoubleArray inverseFarness;
    private ExecutorService executorService;
    private final int concurrency;
    private final long nodeCount;

    public HarmonicCentrality(Graph graph, AllocationTracker allocationTracker, int concurrency, ExecutorService executorService) {
        this.graph = graph;
        this.allocationTracker = allocationTracker;
        this.concurrency = concurrency;
        this.executorService = executorService;
        nodeCount = graph.nodeCount();
        inverseFarness = PagedAtomicDoubleArray.newArray(nodeCount, allocationTracker);
    }

    public HarmonicCentrality compute() {
        final ProgressLogger progressLogger = getProgressLogger();
        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            final double len = sourceNodeIds.size();
            inverseFarness.add(nodeId, len * (1.0 / depth));
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };

        new MultiSourceBFS(
                graph,
                graph,
                Direction.BOTH,
                consumer,
                allocationTracker)
                .run(concurrency, executorService);

        return this;
    }

    public Stream<Result> resultStream() {
        return LongStream.range(0, nodeCount)
                .mapToObj(nodeId -> new Result(
                        graph.toOriginalNodeId(nodeId),
                        inverseFarness.get(nodeId) / (double)(nodeCount - 1)));
    }

    public void export(final String propertyName, final NodePropertyExporter exporter) {
        exporter.write(
                propertyName,
                inverseFarness,
                (PropertyTranslator.OfDouble<PagedAtomicDoubleArray>)
                        (data, nodeId) -> data.get((int) nodeId) / (double) (nodeCount - 1));
    }

    @Override
    public HarmonicCentrality me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
        executorService = null;
        inverseFarness.release();
        inverseFarness = null;
    }

    public final double[] exportToArray() {
        return resultStream()
                .limit(Integer.MAX_VALUE)
                .mapToDouble(r -> r.centrality)
                .toArray();
    }


    /**
     * Result class used for streaming
     */
    public final class Result {

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
