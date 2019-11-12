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

import com.carrotsearch.hppc.AbstractIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.impl.WeightedAllShortestPaths.Result;
import org.neo4j.graphalgo.impl.msbfs.MultiSourceBFS;
import org.neo4j.graphdb.Direction;

import java.util.Iterator;
import java.util.Spliterators;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * AllShortestPaths:
 * <p>
 * multi-source parallel shortest path between each pair of nodes.
 * <p>
 * Due to the high memory footprint the result set would have we emit each result into
 * a blocking queue. The result stream takes elements from the queue while the workers
 * add elements to it.
 */
public class MSBFSAllShortestPaths extends MSBFSASPAlgorithm<MSBFSAllShortestPaths> {

    private Graph graph;
    private BlockingQueue<Result> resultQueue;
    private final AllocationTracker tracker;
    private final int concurrency;
    private final ExecutorService executorService;
    private final Direction direction;
    private final long nodeCount;

    public MSBFSAllShortestPaths(
            Graph graph,
            AllocationTracker tracker,
            int concurrency,
            ExecutorService executorService,
            Direction direction) {
        this.graph = graph;
        nodeCount = graph.nodeCount();
        this.tracker = tracker;
        this.concurrency = concurrency;
        this.executorService = executorService;
        this.direction = direction;
        this.resultQueue = new LinkedBlockingQueue<>(); // TODO limit size?
    }

    /**
     * the resultStream(..) method starts the computation and
     * returns a Stream of SP-Tuples (source, target, minDist)
     *
     * @return the result stream
     */
    @Override
    public Stream<Result> resultStream() {
        executorService.submit(new ShortestPathTask(concurrency, executorService));
        Iterator<Result> iterator = new AbstractIterator<Result>() {
            @Override
            protected Result fetch() {
                try {
                    Result result = resultQueue.take();
                    if (result.sourceNodeId == -1) {
                        return done();
                    }
                    return result;
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            }
        };
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(
                iterator,
                0), false);
    }

    @Override
    public MSBFSAllShortestPaths me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
        resultQueue = null;
    }

    /**
     * Dijkstra Task. Takes one element of the counter at a time
     * and starts dijkstra on it. It starts emitting results to the
     * queue once all reachable nodes have been visited.
     */
    private class ShortestPathTask implements Runnable {

        private final int concurrency;
        private final ExecutorService executorService;

        private ShortestPathTask(
                int concurrency,
                ExecutorService executorService) {
            this.concurrency = concurrency;
            this.executorService = executorService;
        }

        @Override
        public void run() {

            final ProgressLogger progressLogger = getProgressLogger();
            final double maxNodeId = nodeCount - 1;
            new MultiSourceBFS(
                    graph,
                    graph,
                    direction,
                    (target, distance, sources) -> {
                        while (sources.hasNext()) {
                            long source = sources.next();
                            final Result result = new Result(
                                    graph.toOriginalNodeId(source),
                                    graph.toOriginalNodeId(target),
                                    distance);
                            try {
                                resultQueue.put(result);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        progressLogger.logProgress(target, maxNodeId);
                    },
                    tracker
            ).run(concurrency, executorService);

            resultQueue.add(new Result(-1, -1, -1));
        }
    }
}
