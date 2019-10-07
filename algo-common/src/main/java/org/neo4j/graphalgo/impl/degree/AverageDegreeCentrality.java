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
package org.neo4j.graphalgo.impl.degree;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.Pools;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class AverageDegreeCentrality extends Algorithm<AverageDegreeCentrality> {
    private final long nodeCount;
    private Graph graph;
    private final ExecutorService executor;
    private final int concurrency;
    private volatile AtomicLong nodeQueue = new AtomicLong();
    private final Histogram histogram;

    public AverageDegreeCentrality(
            Graph graph,
            ExecutorService executor,
            int concurrency
    ) {
        if (concurrency <= 0) {
            concurrency = Pools.DEFAULT_CONCURRENCY;
        }

        this.graph = graph;
        this.executor = executor;
        this.concurrency = concurrency;
        nodeCount = graph.nodeCount();
        MetricRegistry doubleRecorder = new MetricRegistry();
        this.histogram = doubleRecorder.histogram("stats");
    }

    public AverageDegreeCentrality compute() {
        nodeQueue.set(0);

        List<Runnable> tasks = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
                tasks.add(new DegreeTask());
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, executor);

        return this;
    }

    @Override
    public AverageDegreeCentrality me() {
        return this;
    }

    @Override
    public void release() {
        graph = null;
    }

    private class DegreeTask implements Runnable {
        @Override
        public void run() {
            for (; ; ) {
                final long nodeId = nodeQueue.getAndIncrement();
                if (nodeId >= nodeCount || !running()) {
                    return;
                }

                int degree = graph.degree(nodeId, graph.getLoadDirection());
                histogram.update(degree);
            }
        }
    }

    public double average() {
        return histogram.getSnapshot().getMean();
    }
}
