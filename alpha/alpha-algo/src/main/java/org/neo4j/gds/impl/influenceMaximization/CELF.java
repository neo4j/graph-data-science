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
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.results.InfluenceMaximizationResult;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class CELF extends Algorithm<CELF, CELF> {

    private final long seedSetCount;
    private final double propagationProbability;
    private final int monteCarloSimulations;
    private final int concurrency;

    private final Graph graph;
    private final ArrayList<Runnable> tasks;
    private final LongDoubleScatterMap seedSetNodes;
    private final HugeLongPriorityQueue spreads;
    private final AllocationTracker allocationTracker;
    private final ExecutorService executorService;

    private double gain;

    /*
     * seedSetCount:            Number of seed set nodes
     * monteCarloSimulations:   Number of Monte-Carlo simulations
     * propagationProbability:  Propagation Probability
     */
    public CELF(
        Graph graph,
        int seedSetCount,
        double propagationProbability,
        int monteCarloSimulations,
        ExecutorService executorService,
        int concurrency,
        AllocationTracker allocationTracker
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.graph = graph;
        long nodeCount = graph.nodeCount();

        this.seedSetCount = (seedSetCount <= nodeCount) ? seedSetCount : nodeCount; // k <= nodeCount
        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;

        this.executorService = executorService;
        this.concurrency = concurrency;
        this.allocationTracker = allocationTracker;
        this.tasks = new ArrayList<>();

        seedSetNodes = new LongDoubleScatterMap(seedSetCount);
        spreads = new HugeLongPriorityQueue(nodeCount) {
            @Override
            protected boolean lessThan(long a, long b) {
                return costValues.get(a) != costValues.get(b) ? costValues.get(a) > costValues.get(b) : a < b;
            }
        };
    }

    @Override
    public CELF compute() {
        //Find the first node with greedy algorithm
        greedyPart();
        //Find the next k-1 nodes using the list-sorting procedure
        lazyForwardPart();
        return this;
    }

    private void greedyPart() {
        double highestScore;
        long highestNode;

        var globalNodeProgress = new AtomicLong(0);
        for (int i = 0; i < concurrency; i++) {
            var runner = new IndependentCascadeRunner(graph, spreads, globalNodeProgress,
                propagationProbability,
                monteCarloSimulations,
                allocationTracker
            );
            runner.setSeedSetNodes(new long[0]);
            tasks.add(runner);
        }
        ParallelUtil.runWithConcurrency(concurrency, tasks, executorService);

        //Add the node with the highest spread to the seed set
        highestScore = spreads.cost(spreads.top());
        highestNode = spreads.pop();
        seedSetNodes.put(highestNode, highestScore);
        gain = highestScore;
    }

    private void lazyForwardPart() {
        double highestScore;
        long highestNode;

        var independentCascade = new IndependentCascade(
            graph,
            propagationProbability,
            monteCarloSimulations,
            spreads,
            allocationTracker
        );

        for (long i = 0; i < seedSetCount - 1; i++) {
            do {

                highestNode = spreads.pop();
                //Recalculate the spread of the top node
                independentCascade.run(highestNode, seedSetNodes.keys().toArray());
                spreads.set(highestNode, spreads.cost(highestNode) - gain);

                //Check if previous top node stayed on top after the sort
            } while (highestNode != spreads.top());

            //Add the node with the highest spread to the seed set
            highestScore = spreads.cost(spreads.top());
            highestNode = spreads.pop();
            seedSetNodes.put(highestNode, highestScore + gain);
            gain += highestScore;
        }
    }

    @Override
    public CELF me() {
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
}
