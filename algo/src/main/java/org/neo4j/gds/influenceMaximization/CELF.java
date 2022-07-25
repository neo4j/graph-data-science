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
package org.neo4j.gds.influenceMaximization;

import com.carrotsearch.hppc.LongDoubleScatterMap;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.LongStream;
import java.util.stream.Stream;

public class CELF extends Algorithm<LongDoubleScatterMap> {

    private final long seedSetCount;
    private final double propagationProbability;
    private final int monteCarloSimulations;
    private final int concurrency;

    private final Graph graph;
    private final long initialRandomSeed;
    private final int batchSize;
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
    public CELF(
        Graph graph,
        int seedSetCount,
        double propagationProbability,
        int monteCarloSimulations,
        ExecutorService executorService,
        int concurrency,
        long initialRandomSeed,
        int batchSize
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.graph = graph;
        this.initialRandomSeed = initialRandomSeed;
        this.batchSize = batchSize;
        long nodeCount = graph.nodeCount();

        this.seedSetCount = (seedSetCount <= nodeCount) ? seedSetCount : nodeCount; // k <= nodeCount
        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;

        this.executorService = executorService;
        this.concurrency = concurrency;

        this.seedSetNodes = new LongDoubleScatterMap(seedSetCount);

        this.seedSetNodesArray = new long[seedSetCount];
        this.spreads = new HugeLongPriorityQueue(nodeCount) {
            @Override
            protected boolean lessThan(long a, long b) {
                return (Double.compare(costValues.get(a), costValues.get(b)) == 0) // when equal costs
                    ? a < b                                                        // the smaller node ID is less
                    : costValues.get(a) > costValues.get(b);                       // otherwise compare the costs
            }
        };
    }

    @Override
    public LongDoubleScatterMap compute() {
        //Find the first node with greedy algorithm
        greedyPart();
        //Find the next k-1 nodes using the list-sorting procedure
        lazyForwardPart();

        return seedSetNodes;
    }

    private void greedyPart() {
        HugeDoubleArray singleSpreadArray = HugeDoubleArray.newArray(graph.nodeCount());

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            graph.nodeCount(),
            partition -> new ICInitTask(
                partition,
                graph,
                propagationProbability,
                monteCarloSimulations,
                singleSpreadArray,
                initialRandomSeed
            ),
            Optional.of(Math.toIntExact(graph.nodeCount()) / concurrency)
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
        gain = spreads.cost(highestNode);
        spreads.pop();
        seedSetNodes.put(highestNode, gain);
        seedSetNodesArray[0] = highestNode;
    }

    private void lazyForwardPart() {

        var independentCascade = ICLazyForwardMC.create(
            graph,
            propagationProbability,
            monteCarloSimulations,
            seedSetNodesArray.clone(),
            concurrency,
            executorService,
            initialRandomSeed,
            batchSize
        );

        var lastUpdate = HugeIntArray.newArray(graph.nodeCount());
        long[] firstK = new long[batchSize];
        for (int i = 1; i < seedSetCount; i++) {
            while (lastUpdate.get(spreads.top()) != i) {
                long batchUpperBound = Math.min(batchSize, spreads.size());
                int actualBatchSize = 0;
                for (int j = 0; j < batchUpperBound; ++j) {
                    var nextNodeId = spreads.getIth(j);
                    if (lastUpdate.get(nextNodeId) != i) {
                        firstK[actualBatchSize++] = nextNodeId;
                    }
                }
                independentCascade.runForCandidate(firstK, actualBatchSize);
                for (int j = 0; j < actualBatchSize; ++j) {
                    long nodeId = firstK[j];
                    double value = independentCascade.getSpread(j) / monteCarloSimulations;
                    spreads.set(nodeId, value - gain);
                    lastUpdate.set(nodeId, i);
                }
            }

            //Add the node with the highest spread to the seed set
            var highestScore = spreads.cost(spreads.top());
            var highestNode = spreads.pop();

            seedSetNodes.put(highestNode, highestScore);
            seedSetNodesArray[i] = highestNode;
            gain += highestScore;
            independentCascade.incrementSeedNode(highestNode);

        }
    }

    @Override
    public void release() {
    }

    public Stream<InfluenceMaximizationResult> resultStream() {
        return LongStream.of(seedSetNodes.keys().toArray())
            .mapToObj(node -> new InfluenceMaximizationResult(
                graph.toOriginalNodeId(node),
                seedSetNodes.getOrDefault(node, 0)
            ));
    }
}
