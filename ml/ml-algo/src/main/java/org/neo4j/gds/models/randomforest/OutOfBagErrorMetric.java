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

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.core.concurrency.ParallelUtil;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.utils.paged.HugeAtomicLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeObjectArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.decisiontree.DecisionTreePredict;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public final class OutOfBagErrorMetric {

    private OutOfBagErrorMetric() {}

    static void addPredictionsForTree(
        DecisionTreePredict<Long> decisionTree,
        LocalIdMap classMapping,
        final BitSet sampledFeatureVectors,
        final HugeObjectArray<double[]> allFeatureVectors,
        HugeAtomicLongArray predictions
    ) {
        var totalNumVectors = allFeatureVectors.size();
        var numClasses = classMapping.size();

        for (long i = 0; i < totalNumVectors; i++) {
            if (sampledFeatureVectors.get(i)) continue;

            Long prediction = decisionTree.predict(allFeatureVectors.get(i));
            predictions.getAndAdd(i * numClasses + classMapping.toMapped(prediction), 1);
        }
    }

    public static double evaluate(
        long totalNumVectors,
        LocalIdMap classMapping,
        HugeLongArray expectedLabels,
        int concurrency,
        HugeAtomicLongArray predictions
    ) {
        var totalMistakes = new LongAdder();
        var totalOutOfAnyBagVectors = new LongAdder();

        var tasks = PartitionUtils.rangePartition(concurrency, totalNumVectors, partition ->
                accumulationTask(
                    partition,
                    classMapping,
                    predictions,
                    expectedLabels,
                    totalMistakes,
                    totalOutOfAnyBagVectors
                )
            , Optional.empty());
        ParallelUtil.runWithConcurrency(concurrency, tasks, Pools.DEFAULT);

        return totalMistakes.doubleValue() / totalOutOfAnyBagVectors.doubleValue();
    }

    private static Runnable accumulationTask(
        Partition partition,
        LocalIdMap classMapping,
        HugeAtomicLongArray predictions,
        HugeLongArray expectedLabels,
        LongAdder totalMistakes,
        LongAdder totalOutOfAnyBagVectors
    ) {

        return () -> {
            int numClasses = classMapping.size();
            long numMistakes = 0;
            long numOutOfAnyBagVectors = 0;
            final long startOffset = partition.startNode();
            final long endOffset = startOffset + partition.nodeCount();

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

            totalMistakes.add(numMistakes);
            totalOutOfAnyBagVectors.add(numOutOfAnyBagVectors);
        };
    }
}
