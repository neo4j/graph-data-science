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
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeIntArray;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

import java.util.Optional;
import java.util.concurrent.ExecutorService;

public class CELF extends Algorithm<CELFResult> {

    private final int seedSetCount;
    private final Graph graph;
    private final CELFParameters parameters;
    private final Concurrency concurrency;
    private final LongDoubleScatterMap seedSetNodes;
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
        CELFParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker
    ) {
        super(progressTracker);

        this.graph = graph;
        this.parameters = parameters;
        this.concurrency = parameters.concurrency();
        this.seedSetCount = (parameters.seedSetSize() <= graph.nodeCount())
            ? parameters.seedSetSize()
            : (int) graph.nodeCount(); // k <= nodeCount
        this.executorService = executorService;
        this.seedSetNodes = new LongDoubleScatterMap(seedSetCount);
        this.spreads = new HugeLongPriorityQueue(graph.nodeCount()) {
            @Override
            protected boolean lessThan(long a, long b) {
                return (Double.compare(costValues.get(a), costValues.get(b)) == 0) // when equal costs
                    ? a < b                                                        // the smaller node ID is less
                    : costValues.get(a) > costValues.get(b);                       // otherwise compare the costs
            }
        };

    }


    @Override
    public CELFResult compute() {
        //Find the first node with greedy algorithm
        progressTracker.beginSubTask();
        var firstSeedNode = greedyPart();
        //Find the next k-1 nodes using the list-sorting procedure
        lazyForwardPart(firstSeedNode);
        progressTracker.endSubTask();

        return new CELFResult(seedSetNodes);
    }

    private long greedyPart() {
        HugeDoubleArray singleSpreadArray = HugeDoubleArray.newArray(graph.nodeCount());
        progressTracker.beginSubTask(graph.nodeCount());
        var tasks = PartitionUtils.rangePartition(
            this.concurrency.value(),
            graph.nodeCount(),
            partition -> new ICInitTask(
                partition,
                graph,
                parameters.propagationProbability(),
                parameters.monteCarloSimulations(),
                singleSpreadArray,
                parameters.randomSeed(),
                progressTracker
            ),
            Optional.of(Math.toIntExact(graph.nodeCount()) / this.concurrency.value())
        );

        RunWithConcurrency.builder()
            .concurrency(this.concurrency)
            .tasks(tasks)
            .executor(executorService)
            .run();
        progressTracker.endSubTask();

        graph.forEachNode(nodeId -> {
            spreads.add(nodeId, singleSpreadArray.get(nodeId));
            return true;
        });
        long highestNode = spreads.top();
        gain = spreads.cost(highestNode);
        spreads.pop();
        seedSetNodes.put(highestNode, gain);
        return highestNode;
    }

    private void lazyForwardPart(long firstSeedNode) {

        var independentCascade = ICLazyForwardMC.create(
            graph,
            parameters.propagationProbability(),
            parameters.monteCarloSimulations(),
            firstSeedNode,
            seedSetCount,
            this.concurrency,
            executorService,
            parameters.randomSeed(),
            parameters.batchSize()
        );
        progressTracker.beginSubTask(seedSetCount - 1);
        var lastUpdate = HugeIntArray.newArray(graph.nodeCount());
        long[] firstK = new long[parameters.batchSize()];
        for (int i = 1; i < seedSetCount; i++) {
            while (lastUpdate.get(spreads.top()) != i) {
                long batchUpperBound = Math.min(parameters.batchSize(), spreads.size());
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
                    double value = independentCascade.getSpread(j) / parameters.monteCarloSimulations();
                    spreads.set(nodeId, value - gain);
                    lastUpdate.set(nodeId, i);
                }
            }

            //Add the node with the highest spread to the seed set
            var highestScore = spreads.cost(spreads.top());
            var highestNode = spreads.pop();

            seedSetNodes.put(highestNode, highestScore);
            gain += highestScore;
            independentCascade.incrementSeedNode(highestNode);
            progressTracker.logProgress();
        }
        progressTracker.endSubTask();
    }
}
