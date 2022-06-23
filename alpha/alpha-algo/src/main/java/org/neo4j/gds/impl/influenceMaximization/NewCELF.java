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
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.results.InfluenceMaximizationResult;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class NewCELF extends Algorithm<NewCELF> {

    private final long seedSetCount;
    private final double propagationProbability;
    private final int monteCarloSimulations;
    private final int concurrency;

    private final Graph graph;
    private final LongDoubleScatterMap seedSetNodes;

    private final long[] seedSetNodesArray;
    private final HugeLongPriorityQueue spreads;
    private final ExecutorService executorService;

    private double gain;

    /*
     * seedSetCount:            Number of seed set nodes
     * monteCarloSimulations:   Number of Monte-Carlo simulations
     * propagationProbability:  Propagation Probability
     */
    public NewCELF(
        Graph graph,
        int seedSetCount,
        double propagationProbability,
        int monteCarloSimulations,
        ExecutorService executorService,
        int concurrency
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.graph = graph;
        long nodeCount = graph.nodeCount();

        this.seedSetCount = (seedSetCount <= nodeCount) ? seedSetCount : nodeCount; // k <= nodeCount
        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;

        this.executorService = executorService;
        this.concurrency = concurrency;

        seedSetNodes = new LongDoubleScatterMap(seedSetCount);

        this.seedSetNodesArray = new long[seedSetCount];
        spreads = new HugeLongPriorityQueue(nodeCount) {
            @Override
            protected boolean lessThan(long a, long b) {
                return costValues.get(a) != costValues.get(b) ? costValues.get(a) > costValues.get(b) : a < b;
            }
        };
    }

    @Override
    public NewCELF compute() {
        //Find the first node with greedy algorithm
        greedyPart();
        //Find the next k-1 nodes using the list-sorting procedure
        lazyForwardPart();
        return this;
    }

    private void greedyPart() {
        HugeDoubleArray singleSpreadArray = HugeDoubleArray.newArray(graph.nodeCount());

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> new ICInitThread(
                partition,
                graph,
                propagationProbability,
                monteCarloSimulations,
                singleSpreadArray
            ),
            Optional.of((int) graph.nodeCount() / concurrency)
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executorService)
            .run();

        graph.forEachNode(nodeId -> {
            spreads.add(nodeId, singleSpreadArray.get(nodeId));
            return true;
        });
        long highestNode = spreads.top();
        double highestScore = spreads.cost(highestNode);
        spreads.pop();
        seedSetNodes.put(highestNode, highestScore);
        seedSetNodesArray[0] = highestNode;
        gain = highestScore;
    }

    private void lazyForwardPart() {
        double highestScore;
        long highestNode;


        var independentCascade = new ICLazyForwardMC(
            graph,
            propagationProbability,
            monteCarloSimulations,
            seedSetNodesArray,
            concurrency,
            executorService
        );
        HugeIntArray lastUpdate = HugeIntArray.newArray(graph.nodeCount());
        for (int i = 1; i < seedSetCount; i++) {
            do {

                highestNode = spreads.top();
                if (highestNode == -1) {
                    highestNode = -1;
                }
                //Recalculate the spread of the top node
                double spread = independentCascade.runForCandidate(highestNode);
                spreads.set(highestNode, spread - gain);
                lastUpdate.set(highestNode, i);

                //Check if previous top node stayed on top after the sort
            } while (i != lastUpdate.get(spreads.top()));

            //Add the node with the highest spread to the seed set
            highestScore = spreads.cost(spreads.top());
            highestNode = spreads.pop();

            seedSetNodes.put(highestNode, highestScore + gain);
            seedSetNodesArray[i] = highestNode;
            gain += highestScore;
            independentCascade.incrementSeedNode();
        }
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
