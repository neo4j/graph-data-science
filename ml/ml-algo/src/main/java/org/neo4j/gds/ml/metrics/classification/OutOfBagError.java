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
package org.neo4j.gds.ml.metrics.classification;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.paged.ReadOnlyHugeLongArray;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.partition.PartitionUtils;
import org.neo4j.gds.ml.decisiontree.DecisionTreePredictor;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.models.Features;

import java.util.Comparator;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;

public final class OutOfBagError implements Metric {
    private OutOfBagError() {
    }

    @Override
    public boolean isModelSpecific() {
        return true;
    }

    public static final OutOfBagError OUT_OF_BAG_ERROR = new OutOfBagError();

    public static void addPredictionsForTree(
        DecisionTreePredictor<Integer> decisionTree,
        int numberOfClasses,
        final Features allFeatureVectors,
        ReadOnlyHugeLongArray trainSet,
        final BitSet sampledTrainSet,
        HugeAtomicLongArray predictions
    ) {
        for (long trainSetIdx = 0; trainSetIdx < trainSet.size(); trainSetIdx++) {
            if (sampledTrainSet.get(trainSetIdx)) continue;

            double[] featureVector = allFeatureVectors.get(trainSet.get(trainSetIdx));
            Integer prediction = decisionTree.predict(featureVector);
            predictions.getAndAdd(trainSetIdx * numberOfClasses + prediction, 1);
        }
    }

    public static double evaluate(
        ReadOnlyHugeLongArray trainSet,
        int numberOfClasses,
        HugeIntArray expectedLabels,
        int concurrency,
        HugeAtomicLongArray predictions
    ) {
        var totalMistakes = new LongAdder();
        var totalOutOfAnyBagVectors = new LongAdder();

        var tasks = PartitionUtils.rangePartition(concurrency, trainSet.size(), partition ->
                accumulationTask(
                    partition,
                    numberOfClasses,
                    trainSet,
                    predictions,
                    expectedLabels,
                    totalMistakes,
                    totalOutOfAnyBagVectors
                ),
            Optional.empty()
        );

        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .run();

        if (totalOutOfAnyBagVectors.longValue() == 0L) {
            return 0;
        } else {
            return totalMistakes.doubleValue() / totalOutOfAnyBagVectors.doubleValue();
        }
    }

    private static Runnable accumulationTask(
        Partition partition,
        int numberOfClasses,
        ReadOnlyHugeLongArray trainSet,
        HugeAtomicLongArray predictions,
        HugeIntArray expectedLabels,
        LongAdder totalMistakes,
        LongAdder totalOutOfAnyBagVectors
    ) {

        return () -> {
            long numMistakes = 0;
            long numOutOfAnyBagVectors = 0;
            final long startOffset = partition.startNode();
            final long endOffset = startOffset + partition.nodeCount();

            for (long i = startOffset; i < endOffset; i++) {
                final long innerOffset = i * numberOfClasses;
                long max = 0;
                int maxClassIdx = 0;

                for (int j = 0; j < numberOfClasses; j++) {
                    var numPredictions = predictions.get(innerOffset + j);

                    if (numPredictions <= max) continue;

                    max = numPredictions;
                    maxClassIdx = j;
                }

                if (max == 0) continue;

                // The ith feature vector was in at least one out-of-bag dataset.
                numOutOfAnyBagVectors++;
                if (maxClassIdx != expectedLabels.get(trainSet.get(i))) {
                    numMistakes++;
                }
            }

            totalMistakes.add(numMistakes);
            totalOutOfAnyBagVectors.add(numOutOfAnyBagVectors);
        };
    }

    @Override
    public String name() {
        return "OUT_OF_BAG_ERROR";
    }

    @Override
    public String toString() {
        return name();
    }

    @Override
    public Comparator<Double> comparator() {
        return Comparator.naturalOrder();
    }
}
