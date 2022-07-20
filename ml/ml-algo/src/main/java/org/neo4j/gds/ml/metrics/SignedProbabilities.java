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
package org.neo4j.gds.ml.metrics;

import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeIntArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.splitting.EdgeSplitter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Optional;
import java.util.stream.DoubleStream;

/**
 * Represents a sorted list of doubles, sorted according to their absolute value in increasing order.
 */
public abstract class SignedProbabilities {
    static double ALMOST_ZERO = 1e-100;
    private static final Comparator<Double> ABSOLUTE_VALUE_COMPARATOR = Comparator.comparingDouble(Math::abs);

    private long positiveCount;
    private long negativeCount;

    public static long estimateMemory(long relationshipSetSize) {
        return MemoryUsage.sizeOfInstance(SignedProbabilities.class) +
               MemoryUsage.sizeOfInstance(Optional.class) +
               MemoryUsage.sizeOfInstance(ArrayList.class) +
               MemoryUsage.sizeOfInstance(Double.class) * relationshipSetSize;
    }

    static SignedProbabilities create(long capacity) {
        if (capacity > Integer.MAX_VALUE) {
            return new Huge(capacity);
        } else {
            return new ArrayBased((int) capacity);
        }
    }

    private static final class ArrayBased extends SignedProbabilities {

        private final ArrayList<Double> probabilities;

        private ArrayBased(int capacity) {
            this.probabilities = new ArrayList<>(capacity);
        }

        @Override
        void doAdd(double signedProbability) {
            probabilities.add(signedProbability);
        }

        @Override
        public DoubleStream stream() {
            probabilities.sort(ABSOLUTE_VALUE_COMPARATOR);
            return probabilities.stream().mapToDouble(d -> d);
        }
    }

    static final class Huge extends SignedProbabilities {

        private final HugeDoubleArray probabilities;
        private long index;

        Huge(long capacity) {
            this.probabilities = HugeDoubleArray.newArray(capacity);
            this.index = 0;
        }

        @Override
        void doAdd(double signedProbability) {
            probabilities.set(index++, signedProbability);
        }

        @Override
        public DoubleStream stream() {
            return probabilities.stream().boxed().sorted(ABSOLUTE_VALUE_COMPARATOR).mapToDouble(d -> d);
        }
    }

    public static SignedProbabilities computeFromLabeledData(
        Features features,
        HugeIntArray labels,
        Classifier classifier,
        BatchQueue evaluationQueue,
        int concurrency,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        progressTracker.setSteps(features.size());

        var signedProbabilities = SignedProbabilities.create(evaluationQueue.totalSize());

        var positiveClassIndex = (int) EdgeSplitter.POSITIVE;
        evaluationQueue.parallelConsume(concurrency, __ -> batch -> {
                var probabilityMatrix = classifier.predictProbabilities(batch, features);
                var offset = 0;
                var batchIterator = batch.elementIds();

                while (batchIterator.hasNext()) {
                    var relationshipIdx = batchIterator.nextLong();
                    double probabilityOfPositiveEdge = probabilityMatrix.dataAt(offset++, positiveClassIndex);
                    boolean isEdge = labels.get(relationshipIdx) == EdgeSplitter.POSITIVE;

                    signedProbabilities.add(probabilityOfPositiveEdge, isEdge);
                }
                progressTracker.logSteps(batch.size());
            },
            terminationFlag
        );

        return signedProbabilities;
    }


    public synchronized void add(double probability, boolean isPositive) {
        var nonZeroProbability = probability == 0 ? ALMOST_ZERO : probability;
        var signedProbability = isPositive ? nonZeroProbability : -1 * nonZeroProbability;
        if (signedProbability > 0) {
            positiveCount++;
        } else {
            negativeCount++;
        }

        doAdd(signedProbability);
    }

    abstract void doAdd(double signedProbability);

    public abstract DoubleStream stream();

    public long positiveCount() {
        return positiveCount;
    }

    public long negativeCount() {
        return negativeCount;
    }
}
