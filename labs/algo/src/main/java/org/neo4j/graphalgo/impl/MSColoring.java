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

import com.carrotsearch.hppc.IntIntMap;
import com.carrotsearch.hppc.IntIntScatterMap;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.msbfs.BfsSources;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Multi Source Coloring is a parallel connected Components algorithm
 */
public class MSColoring {

    private final Graph graph;

    private final ExecutorService executorService;

    private final AtomicIntegerArray colors;

    private final int concurrency;
    private int nodeCount;

    public MSColoring(
            Graph graph,
            ExecutorService executorService,
            int concurrency) {
        this.graph = graph;
        this.executorService = executorService;
        nodeCount = Math.toIntExact(graph.nodeCount());
        colors = new AtomicIntegerArray(nodeCount);
        this.concurrency = concurrency;
    }

    public AtomicIntegerArray getColors() {
        return colors;
    }

    public MSColoring compute() {
        // reset state so that each node has its own id as color
        reset();
        // start bfs from all sources (direction does not matter)
        new MultiSourceBFS(graph, graph, Direction.OUTGOING, this::nodeAction, AllocationTracker.EMPTY)
                .run(concurrency, executorService);
        return this;
    }

    public int getSetCount() {
        final IntIntMap map = new IntIntScatterMap();
        for (int i = nodeCount; i >= 0; i--) {
            int color = colors.get(i);
            map.addTo(color, 1);
        }
        return map.size();
    }

    public Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(graph.toOriginalNodeId(i), colors.get(i)));
    }

    private void reset() {
        ParallelUtil.iterateParallel(executorService,
                nodeCount,
                Runtime.getRuntime().availableProcessors() * 2, // TODO constant/getter anywhere!?
                offset -> colors.set(offset, offset));
    }

    private void nodeAction(long nodeId, int depth, BfsSources bfsSources) {
        // This will break for very large graphs
        int node = Math.toIntExact(nodeId);
        // evaluate highest color
        int bestColor = this.colors.get(node);
        while (bfsSources.hasNext()) {
            // This will break for very large graphs
            final int next = Math.toIntExact(bfsSources.next());
            final int sourceColor = colors.get(next);
            bestColor = Math.max(bestColor, sourceColor);
        }
        // set color to target node
        setColor(node, bestColor);
        // reset iterator
        bfsSources.reset();
        // set highest color to all sources
        while (bfsSources.hasNext()) {
            // This will break for very large graphs
            final int source = Math.toIntExact(bfsSources.next());
            setColor(source, bestColor);
        }
    }

    private void setColor(int node, int color) {
        /* loop until either current is higher or equal
           to color or color was successfully saved */
        int current;
        do {
            current = colors.get(node);
        } while (color >= current && !colors.compareAndSet(node, current, color));
    }

    public static class Result {

        public final long nodeId;

        public final long color;

        public Result(long nodeId, int color) {
            this.nodeId = nodeId;
            this.color = color;
        }
    }
}
