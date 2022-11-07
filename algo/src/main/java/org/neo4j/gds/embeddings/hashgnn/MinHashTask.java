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
package org.neo4j.gds.embeddings.hashgnn;


import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.embeddings.hashgnn.HashGNNCompanion.hashArgMin;

class MinHashTask implements Runnable {
    private final List<HashTask.Hashes> hashes;
    private final int k;
    private final HashGNNConfig config;
    private final int embeddingDimension;
    private final List<Graph> concurrentGraphs;
    private final HugeObjectArray<BitSet> currentEmbeddings;
    private final HugeObjectArray<BitSet> previousEmbeddings;
    private final int iteration;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;

    MinHashTask(
        int k,
        List<Graph> graphs,
        HashGNNConfig config,
        int embeddingDimension,
        HugeObjectArray<BitSet> currentEmbeddings,
        HugeObjectArray<BitSet> previousEmbeddings,
        int iteration,
        List<HashTask.Hashes> hashes,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.k = k;
        this.concurrentGraphs = graphs.stream().map(Graph::concurrentCopy).collect(Collectors.toList());
        this.config = config;
        this.embeddingDimension = embeddingDimension;
        this.currentEmbeddings = currentEmbeddings;
        this.previousEmbeddings = previousEmbeddings;
        this.iteration = iteration;
        this.hashes = hashes;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
    }

    static void compute(
        List<Graph> graphs,
        HashGNNConfig config,
        int embeddingDimension,
        HugeObjectArray<BitSet> currentEmbeddings,
        HugeObjectArray<BitSet> previousEmbeddings,
        int iteration,
        List<HashTask.Hashes> hashes,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        progressTracker.beginSubTask("Propagate embeddings iteration");

        progressTracker.setSteps(graphs.get(0).nodeCount());

        var tasks = IntStream.range(0, config.embeddingDensity())
            .mapToObj(k -> new MinHashTask(
                k,
                graphs,
                config,
                embeddingDimension,
                currentEmbeddings,
                previousEmbeddings,
                iteration,
                hashes,
                terminationFlag,
                progressTracker
            ))
            .collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .run();

        progressTracker.endSubTask("Propagate embeddings iteration");
    }

    @Override
    public void run() {
        var neighborsVector = new BitSet(embeddingDimension);
        var selfMinAndArgMin = new HashGNN.MinAndArgmin();
        var neighborsMinAndArgMin = new HashGNN.MinAndArgmin();

        terminationFlag.assertRunning();

        var hashesForK = hashes.get(iteration * config.embeddingDensity() + k);
        var neighborsAggregationHashes = hashesForK.neighborsAggregationHashes();
        var selfAggregationHashes = hashesForK.selfAggregationHashes();
        var preAggregationHashes = hashesForK.preAggregationHashes();


        concurrentGraphs.get(0).forEachNode(nodeId -> {
            var currentEmbedding = currentEmbeddings.get(nodeId);
            hashArgMin(previousEmbeddings.get(nodeId), selfAggregationHashes, selfMinAndArgMin);

            neighborsVector.clear();

            for (int i = 0; i < concurrentGraphs.size(); i++) {
                var preAggregationHashesForRel = preAggregationHashes.get(i);
                var currentGraph = concurrentGraphs.get(i);
                currentGraph.forEachRelationship(nodeId, (src, trg) -> {
                    var prevTargetEmbedding = previousEmbeddings.get(trg);
                    hashArgMin(prevTargetEmbedding, preAggregationHashesForRel, neighborsMinAndArgMin);

                    int argMin = neighborsMinAndArgMin.argMin;
                    if (argMin != -1) {
                        neighborsVector.set(argMin);
                    }

                    return true;
                });
            }

            hashArgMin(neighborsVector, neighborsAggregationHashes, neighborsMinAndArgMin);
            int argMin = (neighborsMinAndArgMin.min < selfMinAndArgMin.min) ? neighborsMinAndArgMin.argMin : selfMinAndArgMin.argMin;
            if (argMin != -1) {
                currentEmbedding.set(argMin);
            }
            return true;

        });

        progressTracker.logSteps(1);
    }
}
