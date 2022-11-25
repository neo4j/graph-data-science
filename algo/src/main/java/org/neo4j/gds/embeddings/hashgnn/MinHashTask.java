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
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.DegreePartition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.neo4j.gds.embeddings.hashgnn.HashGNNCompanion.hashArgMin;

class MinHashTask implements Runnable {
    private final List<HashTask.Hashes> hashes;
    private final int k;
    private final int embeddingDimension;
    private final DegreePartition partition;
    private final List<Graph> concurrentGraphs;
    private final HugeObjectArray<HugeAtomicBitSet> currentEmbeddings;
    private final HugeObjectArray<HugeAtomicBitSet> previousEmbeddings;
    private final TerminationFlag terminationFlag;
    private final ProgressTracker progressTracker;
    private long totalNumFeatures = 0;

    MinHashTask(
        int k,
        DegreePartition partition,
        List<Graph> graphs,
        int embeddingDimension,
        HugeObjectArray<HugeAtomicBitSet> currentEmbeddings,
        HugeObjectArray<HugeAtomicBitSet> previousEmbeddings,
        List<HashTask.Hashes> hashes,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        this.k = k;
        this.partition = partition;
        this.concurrentGraphs = graphs.stream().map(Graph::concurrentCopy).collect(Collectors.toList());
        this.embeddingDimension = embeddingDimension;
        this.currentEmbeddings = currentEmbeddings;
        this.previousEmbeddings = previousEmbeddings;
        this.hashes = hashes;
        this.terminationFlag = terminationFlag;
        this.progressTracker = progressTracker;
    }

    static void compute(
        List<DegreePartition> degreePartition,
        List<Graph> graphs,
        HashGNNConfig config,
        int embeddingDimension,
        HugeObjectArray<HugeAtomicBitSet> currentEmbeddings,
        HugeObjectArray<HugeAtomicBitSet> previousEmbeddings,
        List<HashTask.Hashes> hashes,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag,
        MutableLong totalNumFeaturesOutput
    ) {
        progressTracker.beginSubTask("Perform min-hashing");

        progressTracker.setSteps(config.embeddingDensity() * graphs.get(0).nodeCount());

        var tasks = IntStream.range(0, config.embeddingDensity())
            .mapToObj(k -> degreePartition.stream().map(p ->
                new MinHashTask(
                    k,
                    p,
                    graphs,
                    embeddingDimension,
                    currentEmbeddings,
                    previousEmbeddings,
                    hashes,
                    terminationFlag,
                    progressTracker
                )))
            .flatMap(Function.identity())
            .collect(Collectors.toList());
        RunWithConcurrency.builder()
            .concurrency(config.concurrency())
            .tasks(tasks)
            .terminationFlag(terminationFlag)
            .run();

        totalNumFeaturesOutput.add(tasks.stream().mapToLong(MinHashTask::totalNumFeatures).sum());

        progressTracker.endSubTask("Perform min-hashing");
    }

    @Override
    public void run() {
        var neighborsVector = new BitSet(embeddingDimension);
        var selfMinAndArgMin = new HashGNN.MinAndArgmin();
        var neighborsMinAndArgMin = new HashGNN.MinAndArgmin();
        var tempMinAndArgMin = new HashGNN.MinAndArgmin();

        terminationFlag.assertRunning();

        var hashesForK = hashes.get(k);
        var neighborsAggregationHashes = hashesForK.neighborsAggregationHashes();
        var selfAggregationHashes = hashesForK.selfAggregationHashes();
        var preAggregationHashes = hashesForK.preAggregationHashes();

        partition.consume(nodeId -> {
            var currentEmbedding = currentEmbeddings.get(nodeId);
            hashArgMin(previousEmbeddings.get(nodeId), selfAggregationHashes, selfMinAndArgMin, tempMinAndArgMin);

            neighborsVector.clear();

            for (int i = 0; i < concurrentGraphs.size(); i++) {
                var preAggregationHashesForRel = preAggregationHashes.get(i);
                var currentGraph = concurrentGraphs.get(i);
                currentGraph.forEachRelationship(nodeId, (src, trg) -> {
                    var prevTargetEmbedding = previousEmbeddings.get(trg);
                    hashArgMin(
                        prevTargetEmbedding,
                        preAggregationHashesForRel,
                        neighborsMinAndArgMin,
                        tempMinAndArgMin
                    );

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
                if (!currentEmbedding.getAndSet(argMin)) {
                    totalNumFeatures++;
                }
            }
        });

        progressTracker.logSteps(partition.nodeCount());
    }

    public long totalNumFeatures() {
        return totalNumFeatures;
    }

}
