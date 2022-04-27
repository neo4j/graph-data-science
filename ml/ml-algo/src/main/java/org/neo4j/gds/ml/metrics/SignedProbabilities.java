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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.TerminationFlag;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mem.MemoryUsage;
import org.neo4j.gds.ml.core.batch.BatchQueue;
import org.neo4j.gds.ml.models.Classifier;
import org.neo4j.gds.ml.models.Features;
import org.neo4j.gds.ml.splitting.EdgeSplitter;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.DoubleStream;

/**
 * Represents a sorted list of doubles, sorted according to their absolute value in increasing order.
 */
public final class SignedProbabilities {
    static double ALMOST_ZERO = 1e-100;
    private static final Comparator<Double> ABSOLUTE_VALUE_COMPARATOR = Comparator.comparingDouble(Math::abs);

    private final Optional<TreeSet<Double>> maybeTree;
    private final Optional<List<Double>> maybeList;
    private long positiveCount;
    private long negativeCount;

    public static long estimateMemory(GraphDimensions dimensions, RelationshipType relationshipType, double relationshipFraction) {
        var relationshipCount = dimensions.relationshipCounts().containsKey(relationshipType)
            ? (dimensions.relationshipCounts().get(relationshipType) * relationshipFraction)
            : (dimensions.relCountUpperBound() * relationshipFraction);

        return estimateMemory((long) relationshipCount);
    }

    public static long estimateMemory(long relationshipSetSize) {
        return MemoryUsage.sizeOfInstance(SignedProbabilities.class) +
               MemoryUsage.sizeOfInstance(Optional.class) +
               MemoryUsage.sizeOfInstance(ArrayList.class) +
               MemoryUsage.sizeOfInstance(Double.class) * relationshipSetSize;
    }

    private SignedProbabilities(TreeSet<Double> tree) {
        this.maybeTree = Optional.of(tree);
        this.maybeList = Optional.empty();
    }

    private SignedProbabilities(List<Double> maybeList) {
        this.maybeTree = Optional.empty();
        this.maybeList = Optional.of(maybeList);
    }

    public static SignedProbabilities create(long capacity) {
        if (capacity > Integer.MAX_VALUE) {
            return new SignedProbabilities(new TreeSet<>(ABSOLUTE_VALUE_COMPARATOR));
        } else {
            return new SignedProbabilities(new ArrayList<>((int) capacity));
        }
    }

    public static SignedProbabilities computeFromLabeledData(
        Features features,
        HugeLongArray labels,
        Classifier classifier,
        BatchQueue evaluationQueue,
        int concurrency,
        TerminationFlag terminationFlag,
        ProgressTracker progressTracker
    ) {
        progressTracker.setVolume(features.size());

        var signedProbabilities = SignedProbabilities.create(features.size());

        int positiveClassId = classifier.classIdMap().toMapped((long) EdgeSplitter.POSITIVE);
        evaluationQueue.parallelConsume(concurrency, thread -> batch -> {
                var probabilityMatrix = classifier.predictProbabilities(batch, features);
                var offset = 0;
                for (Long relationshipIdx : batch.nodeIds()) {
                    double probabilityOfPositiveEdge = probabilityMatrix.dataAt(offset, positiveClassId);
                    offset += 1;
                    boolean isEdge = labels.get(relationshipIdx) == EdgeSplitter.POSITIVE;

                    signedProbabilities.add(probabilityOfPositiveEdge, isEdge);
                }
                progressTracker.logProgress(batch.size());
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

        maybeTree.ifPresentOrElse(
            tree -> tree.add(signedProbability),
            () -> maybeList.orElseThrow().add(signedProbability)
        );
    }

    public DoubleStream stream() {
        if (maybeTree.isPresent()) {
            return maybeTree.get().stream().mapToDouble(d -> d);
        } else {
            var list = maybeList.orElseThrow();
            list.sort(ABSOLUTE_VALUE_COMPARATOR);
            return list.stream().mapToDouble(d -> d);
        }
    }

    public long positiveCount() {
        return positiveCount;
    }

    public long negativeCount() {
        return negativeCount;
    }
}
