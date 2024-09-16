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
package org.neo4j.gds.similarity.nodesim;

import org.neo4j.gds.collections.hsa.HugeSparseDoubleArray;
import org.neo4j.gds.core.utils.Intersections;

import java.util.Arrays;

public class JaccardSimilarityComputer implements MetricSimilarityComputer {
    private final double similarityCutoff;

    public JaccardSimilarityComputer(double similarityCutoff) {
        this.similarityCutoff = similarityCutoff;
    }

    @Override
    public double computeSimilarity(long[] vector1, long[] vector2) {
        long intersection = Intersections.intersection3(vector1, vector2);
        long union = vector1.length + vector2.length - intersection;
        double similarity = union == 0 ? 0 : intersection / (double) union;
        return similarity >= similarityCutoff ? similarity : Double.NaN;
    }

    @Override
    public double computeWeightedSimilarity(long[] vector1, long[] vector2, double[] weights1, double[] weights2) {
        assert vector1.length == weights1.length;
        assert vector2.length == weights2.length;

        // It is possible the input vectors to have different lengths.
        // In such cases we need to make sure that elements that are missing get assigned `0.0` weights.
        // To do so, we make use of HugeSparseDoubleArrays
        // where the index is the element from `vector1` and `vector2` and
        // the values are from `weights1` and `weights2`.

        // 1. Find the maximum element from each of the vector arrays
        var vector1MaxElement = Arrays.stream(vector1).max().orElseThrow();
        var vector2MaxElement = Arrays.stream(vector2).max().orElseThrow();
        var maxElement = Math.max(vector1MaxElement, vector2MaxElement);

        // 2. Create HugeSparseDoubleArrays
        var vector1WeightsBuilder = HugeSparseDoubleArray.builder(0d, maxElement);
        for (int i = 0; i < vector1.length; i++) {
            vector1WeightsBuilder.set(vector1[i], weights1[i]);
        }
        var vector1Weights = vector1WeightsBuilder.build();
        var vector2WeightsBuilder = HugeSparseDoubleArray.builder(0d, maxElement);
        for (int i = 0; i < vector2.length; i++) {
            vector2WeightsBuilder.set(vector2[i], weights2[i]);
        }
        var vector2Weights = vector2WeightsBuilder.build();

        // 3. Iterate over the arrays and compute the min and max sums
        var minSum = 0d;
        var maxSum = 0d;
        for (int i = 0; i <= maxElement; i++) {
            var weight1 = vector1Weights.get(i);
            var weight2 = vector2Weights.get(i);
            minSum += Math.min(weight1, weight2);
            maxSum += Math.max(weight1, weight2);
        }

        // 4. Compute the final similarity
        var similarity = minSum / maxSum;
        return similarity >= similarityCutoff ? similarity : Double.NaN;
    }

    static class Builder implements MetricSimilarityComputerBuilder {
        public MetricSimilarityComputer build(double similarityCutoff) {
            return new JaccardSimilarityComputer(similarityCutoff);
        }

        @Override
        public String render() {
            return "JACCARD";
        }
    }
}
