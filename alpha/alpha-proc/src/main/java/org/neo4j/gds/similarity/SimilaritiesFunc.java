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
package org.neo4j.gds.similarity;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.gds.impl.similarity.SimilarityVectorAggregator;
import org.neo4j.gds.core.utils.Intersections;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;
import org.neo4j.values.storable.Values;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.neo4j.gds.impl.similarity.SimilarityVectorAggregator.CATEGORY_KEY;
import static org.neo4j.gds.impl.similarity.SimilarityVectorAggregator.WEIGHT_KEY;
import static org.neo4j.gds.impl.utils.NumberUtils.getDoubleValue;

public class SimilaritiesFunc {

    public static final Predicate<Number> IS_NULL = Predicate.isEqual(null);
    public static final Comparator<Number> NUMBER_COMPARATOR = new NumberComparator();

    @UserFunction("gds.alpha.similarity.jaccard")
    @Description("RETURN gds.alpha.similarity.jaccard(vector1, vector2) - Given two collection vectors, calculate Jaccard similarity")
    public double jaccardSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1 == null || vector2 == null) return 0;
        return jaccard(vector1, vector2);
    }

    @UserFunction("gds.alpha.similarity.cosine")
    @Description("RETURN gds.alpha.similarity.cosine(vector1, vector2) - Given two collection vectors, calculate cosine similarity")
    public double cosineSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1.size() != vector2.size() || vector1.size() == 0) {
            throw new RuntimeException("Vectors must be non-empty and of the same size");
        }

        int len = Math.min(vector1.size(), vector2.size());
        double[] weights1 = new double[len];
        double[] weights2 = new double[len];

        for (int i = 0; i < len; i++) {
            weights1[i] = getDoubleValue(vector1.get(i));
            weights2[i] = getDoubleValue(vector2.get(i));
        }

        return Math.sqrt(Intersections.cosineSquare(weights1, weights2, len));
    }

    @UserAggregationFunction("gds.alpha.similarity.asVector")
    @Description("RETURN gds.alpha.similarity.asVector(map) - Builds a vector of maps containing items and weights")
    public SimilarityVectorAggregator asVector() {
        return new SimilarityVectorAggregator();
    }

    @UserFunction("gds.alpha.similarity.pearson")
    @Description("RETURN gds.alpha.similarity.pearson(vector1, vector2) - Given two collection vectors, calculate pearson similarity")
    public double pearsonSimilarity(@Name("vector1") Object rawVector1, @Name("vector2") Object rawVector2, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) {
        String listType = config.getOrDefault("vectorType", "numbers").toString();

        if (listType.equalsIgnoreCase("maps")) {
            List<Map<String, Object>> vector1 = (List<Map<String, Object>>) rawVector1;
            List<Map<String, Object>> vector2 = (List<Map<String, Object>>) rawVector2;

            LongSet ids = new LongHashSet();

            LongDoubleMap v1Mappings = new LongDoubleHashMap();
            for (Map<String, Object> entry : vector1) {
                Long id = (Long) entry.get(CATEGORY_KEY);
                ids.add(id);
                v1Mappings.put(id, (Double) entry.get(WEIGHT_KEY));
            }

            LongDoubleMap v2Mappings = new LongDoubleHashMap();
            for (Map<String, Object> entry : vector2) {
                Long id = (Long) entry.get(CATEGORY_KEY);
                ids.add(id);
                v2Mappings.put(id, (Double) entry.get(WEIGHT_KEY));
            }

            double[] weights1 = new double[ids.size()];
            double[] weights2 = new double[ids.size()];

            double skipValue = Double.NaN;
            int index = 0;
            for (long id : ids.toArray()) {
                weights1[index] = v1Mappings.getOrDefault(id, skipValue);
                weights2[index] = v2Mappings.getOrDefault(id, skipValue);
                index++;
            }

            return Intersections.pearsonSkip(weights1, weights2, ids.size(), skipValue);
        } else {
            List<Number> vector1 = (List<Number>) rawVector1;
            List<Number> vector2 = (List<Number>) rawVector2;

            if (vector1.size() != vector2.size() || vector1.size() == 0) {
                throw new RuntimeException("Vectors must be non-empty and of the same size");
            }

            int len = vector1.size();
            double[] weights1 = new double[len];
            double[] weights2 = new double[len];

            for (int i = 0; i < len; i++) {
                weights1[i] = getDoubleValue(vector1.get(i));
                weights2[i] = getDoubleValue(vector2.get(i));
            }
            return Intersections.pearson(weights1, weights2, len);
        }
    }

    @UserFunction("gds.alpha.similarity.euclideanDistance")
    @Description("RETURN gds.alpha.similarity.euclideanDistance(vector1, vector2) - Given two collection vectors, calculate the euclidean distance (square root of the sum of the squared differences)")
    public double euclideanDistance(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1.size() != vector2.size() || vector1.size() == 0) {
            throw new RuntimeException("Vectors must be non-empty and of the same size");
        }

        int len = Math.min(vector1.size(), vector2.size());
        double[] weights1 = new double[len];
        double[] weights2 = new double[len];

        for (int i = 0; i < len; i++) {
            weights1[i] = getDoubleValue(vector1.get(i));
            weights2[i] = getDoubleValue(vector2.get(i));
        }

        return Math.sqrt(Intersections.sumSquareDelta(weights1, weights2, len));
    }

    @UserFunction("gds.alpha.similarity.euclidean")
    @Description("RETURN gds.alpha.similarity.euclidean(vector1, vector2) - Given two collection vectors, calculate similarity based on euclidean distance")
    public double euclideanSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        return 1.0D / (1 + euclideanDistance(vector1, vector2));
    }

    @UserFunction("gds.alpha.similarity.overlap")
    @Description("RETURN gds.alpha.similarity.overlap(vector1, vector2) - Given two collection vectors, calculate overlap similarity")
    public double overlapSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1 == null || vector2 == null) return 0;

        HashSet<Number> intersectionSet = new HashSet<>(vector1);
        intersectionSet.retainAll(vector2);
        int intersection = intersectionSet.size();

        long denominator = Math.min(vector1.size(), vector2.size());
        return denominator == 0 ? 0 : (double) intersection / denominator;
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
        vector1.removeIf(IS_NULL);
        vector2.removeIf(IS_NULL);
        vector1.sort(NUMBER_COMPARATOR);
        vector2.sort(NUMBER_COMPARATOR);

        int index1 = 0;
        int index2 = 0;

        int intersection = 0;
        double union = 0;

        while (index1 < vector1.size() && index2 < vector2.size()) {
            Number val1 = vector1.get(index1);
            Number val2 = vector2.get(index2);
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
        union += (vector1.size() - index1) + (vector2.size() - index2);

        return union == 0 ? 1 : intersection / union;
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
