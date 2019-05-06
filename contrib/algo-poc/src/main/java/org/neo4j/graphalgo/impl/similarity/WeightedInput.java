/*
 * Copyright (c) 2017-2019 "Neo4j,"
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
package org.neo4j.graphalgo.impl.similarity;

import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.impl.results.SimilarityResult;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class WeightedInput implements Comparable<WeightedInput>, SimilarityInput {
    private final long id;
    private int itemCount;
    private final double[] weights;
    final int initialSize;

    public WeightedInput(long id, double[] weights, int fullSize, int itemCount) {
        this.initialSize = fullSize;
        this.id = id;
        this.weights = weights;
        this.itemCount = itemCount;
    }

    public WeightedInput(long id, double[] weights, double skipValue) {
        this(id, weights, weights.length, calculateCount(weights, skipValue));
    }

    public WeightedInput(long id, double[] weights) {
        this(id, weights, weights.length, weights.length);
    }

    private static int calculateCount(double[] weights, double skipValue) {
        boolean skipNan = Double.isNaN(skipValue);
        int count = 0;
        for (double weight : weights) {
            if (!(weight == skipValue || (skipNan && Double.isNaN(weight)))) count++;
        }
        return count;
    }

    public static WeightedInput sparse(long id, double[] weights, int fullSize, int compressedSize) {
        return new WeightedInput(id, weights, fullSize, compressedSize);
    }

    public static WeightedInput dense(long id, double[] weights, double skipValue) {
        return new WeightedInput(id, weights, skipValue);
    }

    public static WeightedInput dense(long id, double[] weights) {
        return new WeightedInput(id, weights);
    }

    static  WeightedInput[] prepareDenseWeights(List<Map<String, Object>> data, long degreeCutoff, Double skipValue) {
        WeightedInput[] inputs = new WeightedInput[data.size()];
        int idx = 0;

        boolean skipAnything = skipValue != null;
        boolean skipNan = skipAnything && Double.isNaN(skipValue);

        for (Map<String, Object> row : data) {
            List<Number> weightList = SimilarityInput.extractValues(row.get("weights"));

            long weightsSize = skipAnything ? skipSize(skipValue, skipNan, weightList) : weightList.size();

            if (weightsSize > degreeCutoff) {
                double[] weights = Weights.buildWeights(weightList);
                inputs[idx++] = skipValue == null ? dense((Long) row.get("item"), weights) : dense((Long) row.get("item"), weights, skipValue);
            }
        }
        if (idx != inputs.length) inputs = Arrays.copyOf(inputs, idx);
        Arrays.sort(inputs);
        return inputs;
    }

    private static long skipSize(Double skipValue, boolean skipNan, List<Number> weightList) {
        return weightList.stream().filter(value -> !Intersections.shouldSkip(value.doubleValue(), skipValue, skipNan)).count();
    }

    public int compareTo(WeightedInput o) {
        return Long.compare(id, o.id);
    }

    public SimilarityResult sumSquareDeltaSkip(RleDecoder decoder, double similarityCutoff, WeightedInput other, double skipValue, boolean bidirectional) {
        double[] thisWeights = weights;
        double[] otherWeights = other.weights;
        if (decoder != null) {
            decoder.reset(weights, other.weights);
            thisWeights = decoder.item1();
            otherWeights = decoder.item2();
        }

        int len = Math.min(thisWeights.length, otherWeights.length);
        double sumSquareDelta = Intersections.sumSquareDeltaSkip(thisWeights, otherWeights, len, skipValue);
        long intersection = 0;

        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id, other.id, itemCount, other.itemCount, intersection, sumSquareDelta,bidirectional, false);
    }

    public SimilarityResult sumSquareDelta(RleDecoder decoder, double similarityCutoff, WeightedInput other, boolean bidirectional) {
        double[] thisWeights = weights;
        double[] otherWeights = other.weights;
        if (decoder != null) {
            decoder.reset(weights, other.weights);
            thisWeights = decoder.item1();
            otherWeights = decoder.item2();
        }

        int len = Math.min(thisWeights.length, otherWeights.length);
        double sumSquareDelta = Intersections.sumSquareDelta(thisWeights, otherWeights, len);
        long intersection = 0;

        if (similarityCutoff >= 0d && sumSquareDelta > similarityCutoff) return null;
        return new SimilarityResult(id, other.id, itemCount, other.itemCount, intersection, sumSquareDelta, bidirectional, false);
    }

    public SimilarityResult cosineSquaresSkip(RleDecoder decoder, double similarityCutoff, WeightedInput other, double skipValue, boolean bidirectional) {
        double[] thisWeights = weights;
        double[] otherWeights = other.weights;
        if (decoder != null) {
            decoder.reset(weights, other.weights);
            thisWeights = decoder.item1();
            otherWeights = decoder.item2();
        }

        int len = Math.min(thisWeights.length, otherWeights.length);
        double cosineSquares = Intersections.cosineSquareSkip(thisWeights, otherWeights, len, skipValue);
        long intersection = 0;

        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, itemCount, other.itemCount, intersection, cosineSquares, bidirectional, false);
    }

    public SimilarityResult cosineSquares(RleDecoder decoder, double similarityCutoff, WeightedInput other, boolean bidirectional) {
        double[] thisWeights = weights;
        double[] otherWeights = other.weights;
        if (decoder != null) {
            decoder.reset(weights, other.weights);
            thisWeights = decoder.item1();
            otherWeights = decoder.item2();
        }

        int len = Math.min(thisWeights.length, otherWeights.length);
        double cosineSquares = Intersections.cosineSquare(thisWeights, otherWeights, len);
        long intersection = 0;

        if (similarityCutoff >= 0d && (cosineSquares == 0 || cosineSquares < similarityCutoff)) return null;
        return new SimilarityResult(id, other.id, itemCount, other.itemCount, intersection, cosineSquares, bidirectional, false);
    }

    public SimilarityResult pearson(RleDecoder decoder, double similarityCutoff, WeightedInput other, boolean bidirectional) {
        double[] thisWeights = weights;
        double[] otherWeights = other.weights;
        if (decoder != null) {
            decoder.reset(weights, other.weights);
            thisWeights = decoder.item1();
            otherWeights = decoder.item2();
        }

        int len = Math.min(thisWeights.length, otherWeights.length);
        double pearson = Intersections.pearson(thisWeights, otherWeights, len);

        if (similarityCutoff >= 0d && (pearson == 0 || pearson < similarityCutoff)) return null;

        return new SimilarityResult(id, other.id, itemCount, other.itemCount, 0, pearson, bidirectional, false);
    }

    public SimilarityResult pearsonSkip(RleDecoder decoder, double similarityCutoff, WeightedInput other, Double skipValue, boolean bidirectional) {
        double[] thisWeights = weights;
        double[] otherWeights = other.weights;
        if (decoder != null) {
            decoder.reset(weights, other.weights);
            thisWeights = decoder.item1();
            otherWeights = decoder.item2();
        }

        int len = Math.min(thisWeights.length, otherWeights.length);
        double pearson = Intersections.pearsonSkip(thisWeights, otherWeights, len, skipValue);

        if (similarityCutoff >= 0d && (pearson == 0 || pearson < similarityCutoff)) return null;

        return new SimilarityResult(id, other.id, itemCount, other.itemCount, 0, pearson, bidirectional, false);
    }

    @Override
    public long getId() {
        return id;
    }
}
