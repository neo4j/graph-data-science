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
package org.neo4j.gds.impl.influenceMaximization;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.results.InfluenceMaximizationResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class Greedy extends Algorithm<Greedy, Greedy> {
    private final Graph graph;
    private final int seedSetCount;
    private final double propagationProbability;
    private final int monteCarloSimulations;

    private final ExecutorService executorService;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final ArrayList<IndependentCascadeRunner> tasks;

    private final LongDoubleScatterMap seedSetNodes;
    private final HugeLongPriorityQueue spreads;
    private final AtomicLong globalNodeProgress;

    /*
     * seedSetCount:            Number of seed set nodes
     * monteCarloSimulations:   Number of Monte-Carlo simulations
     * propagationProbability:  Propagation Probability
     */
    public Greedy(
        Graph graph,
        int seedSetCount,
        double propagationProbability,
        int monteCarloSimulations,
        ExecutorService executorService,
        int concurrency,
        AllocationTracker tracker
    ) {
        this.graph = graph;
        long nodeCount = graph.nodeCount();

        this.seedSetCount = (seedSetCount <= nodeCount) ? seedSetCount : (int) nodeCount;
        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;

        this.executorService = executorService;
        this.concurrency = concurrency;
        this.tracker = tracker;

        this.seedSetNodes = new LongDoubleScatterMap(this.seedSetCount);
        this.spreads = new HugeLongPriorityQueue(nodeCount) {
            @Override
            protected boolean lessThan(long a, long b) {
                return (costValues.get(a) != costValues.get(b)) ? costValues.get(a) > costValues.get(b) : a < b;
            }
        };

        this.globalNodeProgress = new AtomicLong(0);
        this.tasks = initializeTasks();
    }

    @Override
    public Greedy compute() {
        double highestScore;
        long highestNode;

        //Find k nodes with largest marginal gain
        for (long i = 0; i < seedSetCount; i++) {
            globalNodeProgress.set(0);
            spreads.clear();

            tasks.forEach(task -> task.setSeedSetNodes(seedSetNodes.keys().toArray()));
            ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);

            highestScore = spreads.cost(spreads.top());
            highestNode = spreads.pop();
            seedSetNodes.put(highestNode, highestScore);
        }

        return this;
    }

    @Override
    public Greedy me() {
        return this;
    }

    @Override
    public void release() {
    }

    public double getNodeSpread(long node) {
        return seedSetNodes.getOrDefault(node, 0);
    }

    public Stream<InfluenceMaximizationResult> resultStream() {
        return LongStream.of(seedSetNodes.keys().toArray())
            .mapToObj(node -> new InfluenceMaximizationResult(graph.toOriginalNodeId(node), getNodeSpread(node)));
    }

    private ArrayList<IndependentCascadeRunner> initializeTasks() {
        var tasks = new ArrayList<IndependentCascadeRunner>();
        for (int ignore = 0; ignore < concurrency; ignore++) {
            tasks.add(new IndependentCascadeRunner(
                graph,
                spreads,
                globalNodeProgress,
                propagationProbability,
                monteCarloSimulations,
                tracker
            ));
        }
        return tasks;
    }
}
