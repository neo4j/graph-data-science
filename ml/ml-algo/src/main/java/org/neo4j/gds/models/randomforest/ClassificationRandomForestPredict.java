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
package org.neo4j.gds.models.randomforest;

import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeByteArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.decisiontree.DecisionTreePredict;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;

public class ClassificationRandomForestPredict {

    private final DecisionTreePredict<Long>[] decisionTrees;
    private final LocalIdMap classMapping;
    private final int concurrency;

    public ClassificationRandomForestPredict(
        DecisionTreePredict<Long>[] decisionTrees,
        LocalIdMap classMapping,
        int concurrency
    ) {
        this.decisionTrees = decisionTrees;
        this.classMapping = classMapping;
        this.concurrency = concurrency;
    }

    public long predict(final double[] features) {
        final var predictionsPerClass = new AtomicIntegerArray(classMapping.size());
        var tasks = ParallelUtil.tasks(decisionTrees.length, index -> () -> {
            var tree = decisionTrees[index];
            var prediction = tree.predict(features);
            predictionsPerClass.incrementAndGet(classMapping.toMapped(prediction));
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

        return classMapping.toOriginal(maxClassIdx);
    }

    public double outOfBagError(
        final HugeByteArray[] bootstrappedDatasets,
        final HugeObjectArray<double[]> allFeatureVectors,
        final HugeLongArray expectedLabels
    ) {
        assert bootstrappedDatasets.length == decisionTrees.length;
        assert allFeatureVectors.size() == expectedLabels.size();

        final var totalNumVectors = allFeatureVectors.size();
        final var numClasses = classMapping.size();
        final var predictions = HugeAtomicLongArray.newArray(
            classMapping.size() * bootstrappedDatasets[0].size()
        );

        var predictionTasks = ParallelUtil.tasks(decisionTrees.length, index -> () -> {
            final var tree = decisionTrees[index];
            final var bootstrappedDataset = bootstrappedDatasets[index];

            for (long i = 0; i < totalNumVectors; i++) {
                if (bootstrappedDataset.get(i) == (byte) 1) continue;

                Long prediction = tree.predict(allFeatureVectors.get(i));
                predictions.getAndAdd(i * numClasses + classMapping.toMapped(prediction), 1);
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
                if (classMapping.toOriginal(maxClassIdx) != expectedLabels.get(i)) {
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
