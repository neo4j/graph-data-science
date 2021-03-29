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
package org.neo4j.graphalgo.impl.influenceMaximization;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.concurrency.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.graphalgo.results.InfluenceMaximizationResult;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class CELF extends Algorithm<CELF, CELF> {
    private final Graph graph;
    private final long seedSetCount;
    private final double propagationProbability;
    private final int monteCarloSimulations;

    private final ExecutorService executorService;
    private final int concurrency;
    private final AllocationTracker tracker;
    private final ArrayList<Runnable> tasks;

    private final LongDoubleScatterMap seedSetNodes;
    private final HugeLongPriorityQueue spreads;

    private double gain;

    /*
     * seedSetCount:            Number of seed set nodes
     * monteCarloSimulations:   Number of Monte-Carlo simulations
     * propagationProbability:  Propagation Probability
     */
    public CELF(Graph graph, int seedSetCount, double propagationProbability, int monteCarloSimulations, ExecutorService executorService, int concurrency, AllocationTracker tracker) {
        this.graph = graph;
        long nodeCount = graph.nodeCount();

        this.seedSetCount = (seedSetCount <= nodeCount) ? seedSetCount : nodeCount; // k <= nodeCount
        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;

        this.executorService = executorService;
        this.concurrency = concurrency;
        this.tracker = tracker;
        this.tasks = new ArrayList<>();

        seedSetNodes = new LongDoubleScatterMap(seedSetCount);
        spreads = new HugeLongPriorityQueue(nodeCount) {
            @Override
            protected boolean lessThan(long a, long b) {
                return (costValues.get(a) != costValues.get(b)) ? costValues.get(a) > costValues.get(b) : a < b;
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

        tasks.clear();
        //Calculate the first iteration sorted list
        graph.forEachNode(
                node ->
                {
                    if (!seedSetNodes.containsKey(node)) {
                        tasks.add(new IndependentCascadeTask(graph, propagationProbability, monteCarloSimulations, node, seedSetNodes.keys().toArray(), spreads, tracker));
                    }
                    return true;
                });
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

        for (long i = 0; i < seedSetCount - 1; i++) {
            do {
                highestNode = spreads.pop();
                //Recalculate the spread of the top node
                ParallelUtil.run(new IndependentCascadeTask(graph, propagationProbability, monteCarloSimulations, highestNode, seedSetNodes.keys().toArray(), spreads, tracker), executorService);
                spreads.set(highestNode, spreads.cost(highestNode) - gain);
            }//Check if previous top node stayed on top after the sort
            while (highestNode != spreads.top());

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
