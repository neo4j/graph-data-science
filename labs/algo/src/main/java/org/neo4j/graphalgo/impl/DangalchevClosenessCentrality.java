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

package org.neo4j.graphalgo.impl;

import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.AtomicDoubleArray;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.NodePropertyExporter;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.impl.msbfs.BfsConsumer;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Dangalchev Closeness Centrality
 */
public class DangalchevClosenessCentrality extends Algorithm<DangalchevClosenessCentrality> {

    private Graph graph;
    private AtomicDoubleArray farness;

    private final int concurrency;
    private final ExecutorService executorService;
    private final int nodeCount;

    public DangalchevClosenessCentrality(Graph graph, int concurrency, ExecutorService executorService) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        this.concurrency = concurrency;
        this.executorService = executorService;
        farness = new AtomicDoubleArray(nodeCount);
    }

    public DangalchevClosenessCentrality compute() {

        final ProgressLogger progressLogger = getProgressLogger();

        final BfsConsumer consumer = (nodeId, depth, sourceNodeIds) -> {
            int len = sourceNodeIds.size();
            farness.add(Math.toIntExact(nodeId), len * 1.0 / Math.pow(2, depth));
            progressLogger.logProgress((double) nodeId / (nodeCount - 1));
        };

        new MultiSourceBFS(graph, graph, Direction.OUTGOING, consumer, AllocationTracker.EMPTY)
            .run(concurrency, executorService);

        return this;
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
            .mapToObj(nodeId -> new Result(
                graph.toOriginalNodeId(nodeId),
                farness.get(nodeId)));
    }

    public void export(final String propertyName, final NodePropertyExporter exporter) {
        exporter.write(
            propertyName,
            farness,
            (PropertyTranslator.OfDouble<AtomicDoubleArray>)
                (data, nodeId) -> data.get((int) nodeId));
    }

    @Override
    public DangalchevClosenessCentrality me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
        farness = null;
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
    }
}
