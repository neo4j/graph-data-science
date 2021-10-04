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
package org.neo4j.gds.ml.core.randomforest;

import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.ml.core.decisiontree.DecisionTreePredict;

import java.util.Map;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

public class ClassificationRandomForestPredict {

    private final DecisionTreePredict<Integer>[] decisionTrees;
    private final int[] classes;
    private final Map<Integer, Integer> classToIdx;
    private final int concurrency;
    private final AllocationTracker allocationTracker;

    public ClassificationRandomForestPredict(
        DecisionTreePredict<Integer>[] decisionTrees,
        int[] classes,
        Map<Integer, Integer> classToIdx,
        int concurrency,
        AllocationTracker allocationTracker
    ) {
        this.decisionTrees = decisionTrees;
        this.classes = classes;
        this.concurrency = concurrency;
        this.classToIdx = classToIdx;
        this.allocationTracker = allocationTracker;
    }

    public int predict(final double[] features) {
        final var predictionsPerClass = new AtomicIntegerArray(classes.length);
        var tasks = ParallelUtil.tasks(decisionTrees.length, index -> () -> {
            var tree = decisionTrees[index];
            var prediction = tree.predict(features);
            predictionsPerClass.incrementAndGet(classToIdx.get(prediction));
        });
        ParallelUtil.runWithConcurrency(concurrency, tasks, Pools.DEFAULT);

        int max = -1;
        int maxClassIdx = 0;

        for (int i = 0; i < predictionsPerClass.length(); i++) {
            var numPredictions = predictionsPerClass.get(i);

            if (numPredictions <= max) continue;

            max = numPredictions;
            maxClassIdx = i;
        }

        return classes[maxClassIdx];
    }

    public double outOfBagError(
        final HugeByteArray[] bootstrappedDatasets,
        final HugeObjectArray<double[]> allFeatureVectors,
        final HugeIntArray allLabels
    ) {
        assert bootstrappedDatasets.length == decisionTrees.length;
        assert allFeatureVectors.size() == allLabels.size();

        final var totalNumVectors = allFeatureVectors.size();
        final var numClasses = classes.length;
        final var predictions = HugeAtomicLongArray.newArray(
            classes.length * bootstrappedDatasets[0].size(),
            allocationTracker
        );

        var predictionTasks = ParallelUtil.tasks(decisionTrees.length, index -> () -> {
            final var tree = decisionTrees[index];
            final var bootstrappedDataset = bootstrappedDatasets[index];

            for (long i = 0; i < totalNumVectors; i++) {
                if (bootstrappedDataset.get(i) == (byte) 1) continue;

                var prediction = tree.predict(allFeatureVectors.get(i));
                predictions.getAndAdd(i * numClasses + classToIdx.get(prediction), 1);
            }
        });
        ParallelUtil.runWithConcurrency(concurrency, predictionTasks, Pools.DEFAULT);

        var totalMistakes = new AtomicLong(0);
        var totalOutOfAnyBagVectors = new AtomicLong(0);
        long batchSize = totalNumVectors / concurrency;
        long remainder = Math.floorMod(totalNumVectors, concurrency);

        var majorityVoteTasks = ParallelUtil.tasks(concurrency, index -> () -> {
            long numMistakes = 0;
            long numOutOfAnyBagVectors = 0;
            final long startOffset = index * batchSize;
            final long endOffset = index == concurrency - 1
                ? startOffset + batchSize + remainder
                : startOffset + batchSize;

            for (long i = startOffset; i < endOffset; i++) {
                final long innerOffset = i * numClasses;
                long max = 0;
                int maxClassIdx = 0;

                for (int j = 0; j < numClasses; j++) {
                    var numPredictions = predictions.get(innerOffset + j);

                    if (numPredictions <= max) continue;

                    max = numPredictions;
                    maxClassIdx = j;
                }

                if (max == 0) continue;

                // The ith feature vector was in at least one out-of-bag dataset.
                numOutOfAnyBagVectors++;
                if (classes[maxClassIdx] != allLabels.get(i)) {
                    numMistakes++;
                }
            }

            totalMistakes.addAndGet(numMistakes);
            totalOutOfAnyBagVectors.addAndGet(numOutOfAnyBagVectors);
        });
        ParallelUtil.runWithConcurrency(concurrency, majorityVoteTasks, Pools.DEFAULT);

        return totalMistakes.doubleValue() / totalOutOfAnyBagVectors.doubleValue();
    }
}
