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
package org.neo4j.gds.functions;

import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.Values;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

public class SimilaritiesFunc {

    private static final Predicate<Number> IS_NULL = Predicate.isEqual(null);
    private static final Comparator<Number> NUMBER_COMPARATOR = new NumberComparator();
    private static final String CATEGORY_KEY = "category";
    private static final String WEIGHT_KEY = "weight";

    @UserFunction("gds.similarity.jaccard")
    @Description("RETURN gds.similarity.jaccard(vector1, vector2) - Given two collection vectors, calculate Jaccard similarity")
    public double jaccardSimilarity(
        @Name("vector1") List<Number> vector1,
        @Name("vector2") List<Number> vector2
    ) {
        if (vector1 == null || vector2 == null) {
            return 0;
        }
        return jaccard(vector1, vector2);
    }

    @UserFunction("gds.similarity.cosine")
    @Description("RETURN gds.similarity.cosine(vector1, vector2) - Given two collection vectors, calculate cosine similarity")
    public double cosineSimilarity(
        @Name("vector1") List<Number> vector1,
        @Name("vector2") List<Number> vector2
    ) {
        int len = validateLength(vector1, vector2);
        var left = toArray(vector1);
        var right = toArray(vector2);
        return Intersections.cosine(left, right, len);
    }

    @UserFunction("gds.similarity.pearson")
    @Description("RETURN gds.similarity.pearson(vector1, vector2) - Given two collection vectors, calculate pearson similarity")
    public double pearsonSimilarity(
        @Name("vector1") List<Number> vector1,
        @Name("vector2") List<Number> vector2
    ) {
        int len = validateLength(vector1, vector2);
        var left = toArray(vector1);
        var right = toArray(vector2);
        return Intersections.pearson(left, right, len);
    }

    @UserFunction("gds.similarity.euclideanDistance")
    @Description("RETURN gds.similarity.euclideanDistance(vector1, vector2) - Given two collection vectors, calculate the euclidean distance (square root of the sum of the squared differences)")
    public double euclideanDistance(
        @Name("vector1") List<Number> vector1,
        @Name("vector2") List<Number> vector2
    ) {
        int len = validateLength(vector1, vector2);
        var left = toArray(vector1);
        var right = toArray(vector2);
        return Math.sqrt(Intersections.sumSquareDelta(left, right, len));
    }

    @UserFunction("gds.similarity.euclidean")
    @Description("RETURN gds.similarity.euclidean(vector1, vector2) - Given two collection vectors, calculate similarity based on euclidean distance")
    public double euclideanSimilarity(
        @Name("vector1") List<Number> vector1,
        @Name("vector2") List<Number> vector2
    ) {
        return 1.0D / (1 + euclideanDistance(vector1, vector2));
    }

    @UserFunction("gds.similarity.overlap")
    @Description("RETURN gds.similarity.overlap(vector1, vector2) - Given two collection vectors, calculate overlap similarity")
    public double overlapSimilarity(
        @Name("vector1") List<Number> vector1,
        @Name("vector2") List<Number> vector2
    ) {
        vector1.removeIf(IS_NULL);
        vector2.removeIf(IS_NULL);

        if (vector1 == null || vector2 == null) {
            return 0;
        }

        var intersectionSet = new HashSet<>(vector1);
        intersectionSet.retainAll(vector2);
        int intersection = intersectionSet.size();

        long denominator = Math.min(vector1.size(), vector2.size());
        return denominator == 0 ? 0 : (double) intersection / denominator;
    }

    private double[] toArray(List<Number> input) {
        var length = input.size();
        var weights = new double[length];
        for (int i = 0; i < length; i++) {
            weights[i] = getDoubleValue(input.get(i));
        }
        return weights;
    }

    private int validateLength(List<Number> vector1, List<Number> vector2) {
        if (vector1.size() != vector2.size() || vector1.isEmpty()) {
            throw new RuntimeException("Vectors must be non-empty and of the same size");
        }
        return vector1.size();
    }

    /**
     * A jaccard implementation that supports duplicates.
     *
     * We sort the inputs and then loop through them together.
     * For each pair of indexes:
     *   if the value is the same, we increment the intersection and the union, and both indexes
     *   if the value on the left is smaller, we increment the union and the left index
     *   if the value on the right is smaller, we increment the union and the right index
     *
     * We get Lists of Number in and we don't know if these are integers or floats, or at what precision.
     * For this we have a custom comparator for sorting and comparing.
     *
     * Because we increment the side with the smaller number (or both if equal) we may have a remainder on one side.
     * This remainder cannot contribute to the intersection, so we add the size of it to the union.
     *
     * @param vector1 A list of numbers to compare.
     * @param vector2 A list of numbers to compare.
     * @return The jaccard score, the intersection divided by the union of the input lists.
     */
    private double jaccard(List<Number> vector1, List<Number> vector2) {
        var sortedVector1 = removeNullsAndSort(vector1);
        var sortedVector2 = removeNullsAndSort(vector2);

        int index1 = 0;
        int index2 = 0;

        int intersection = 0;
        double union = 0;

        while (index1 < sortedVector1.size() && index2 < sortedVector2.size()) {
            Number val1 = sortedVector1.get(index1);
            Number val2 = sortedVector2.get(index2);
            int compare = NUMBER_COMPARATOR.compare(val1, val2);

            if (compare == 0) {
                intersection++;
                union++;
                index1++;
                index2++;
            } else if (compare < 0) {
                union++;
                index1++;
            } else {
                union++;
                index2++;
            }
        }

        // the remainder, if any, is never shared so add to the union
        union += (sortedVector1.size() - index1) + (sortedVector2.size() - index2);

        return union == 0 ? 1 : intersection / union;
    }

    private static List<Number> removeNulls(List<Number> input) {
        var output = new ArrayList<>(input);
        output.removeIf(IS_NULL);
        return output;
    }

    private static List<Number> removeNullsAndSort(List<Number> input) {
        var output = removeNulls(input);
        output.sort(NUMBER_COMPARATOR);
        return output;
    }

    private static double getDoubleValue(Number value) {
        return Optional.ofNullable(value).map(Number::doubleValue).orElse(0D);
    }

    static class NumberComparator implements Comparator<Number> {

        @Override
        public int compare(Number o1, Number o2) {
            if (o1 instanceof Long && o2 instanceof Long) {
                return ((Long) o1).compareTo((Long) o2);
            }

            if (o1 instanceof Long) {
                return Values.longValue(o1.longValue()).compareTo(Values.doubleValue(o2.doubleValue()));
            }
            if (o2 instanceof Long) {
                return Values.doubleValue(o1.doubleValue()).compareTo(Values.longValue(o2.longValue()));
            }

            return Double.compare(o1.doubleValue(), o2.doubleValue());
        }
    }
}
