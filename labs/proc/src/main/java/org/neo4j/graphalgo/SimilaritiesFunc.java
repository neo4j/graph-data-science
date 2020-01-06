/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo;

import com.carrotsearch.hppc.LongDoubleHashMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.impl.similarity.SimilarityVectorAggregator;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserAggregationFunction;
import org.neo4j.procedure.UserFunction;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.neo4j.graphalgo.impl.similarity.SimilarityVectorAggregator.CATEGORY_KEY;
import static org.neo4j.graphalgo.impl.similarity.SimilarityVectorAggregator.WEIGHT_KEY;

public class SimilaritiesFunc {

    @UserFunction("algo.similarity.jaccard")
    @Description("algo.similarity.jaccard([vector1], [vector2]) " +
            "given two collection vectors, calculate jaccard similarity")
    public double jaccardSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1 == null || vector2 == null) return 0;

        HashSet<Number> intersectionSet = new HashSet<>(vector1);
        intersectionSet.retainAll(vector2);
        int intersection = intersectionSet.size();

        long denominator = vector1.size() + vector2.size() - intersection;
        return denominator == 0 ? 0 : (double) intersection / denominator;
    }

    @UserFunction("algo.similarity.cosine")
    @Description("algo.similarity.cosine([vector1], [vector2]) " +
            "given two collection vectors, calculate cosine similarity")
    public double cosineSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1.size() != vector2.size() || vector1.size() == 0) {
            throw new RuntimeException("Vectors must be non-empty and of the same size");
        }

        int len = Math.min(vector1.size(), vector2.size());
        double[] weights1 = new double[len];
        double[] weights2 = new double[len];

        for (int i = 0; i < len; i++) {
            weights1[i] = vector1.get(i).doubleValue();
            weights2[i] = vector2.get(i).doubleValue();
        }

        return Math.sqrt(Intersections.cosineSquare(weights1, weights2, len));
    }

    @UserAggregationFunction("algo.similarity.asVector")
    @Description("algo.similarity.asVector - builds a vector of maps containing items and weights")
    public SimilarityVectorAggregator asVector() {
        return new SimilarityVectorAggregator();
    }

    @UserFunction("algo.similarity.pearson")
    @Description("algo.similarity.pearson([vector1], [vector2]) " +
            "given two collection vectors, calculate pearson similarity")
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
                weights1[i] = vector1.get(i).doubleValue();
                weights2[i] = vector2.get(i).doubleValue();
            }
            return Intersections.pearson(weights1, weights2, len);
        }
    }

    @UserFunction("algo.similarity.euclideanDistance")
    @Description("algo.similarity.euclideanDistance([vector1], [vector2]) " +
            "given two collection vectors, calculate the euclidean distance (square root of the sum of the squared differences)")
    public double euclideanDistance(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1.size() != vector2.size() || vector1.size() == 0) {
            throw new RuntimeException("Vectors must be non-empty and of the same size");
        }

        int len = Math.min(vector1.size(), vector2.size());
        double[] weights1 = new double[len];
        double[] weights2 = new double[len];

        for (int i = 0; i < len; i++) {
            weights1[i] = vector1.get(i).doubleValue();
            weights2[i] = vector2.get(i).doubleValue();
        }

        return Math.sqrt(Intersections.sumSquareDelta(weights1, weights2, len));
    }

    @UserFunction("algo.similarity.euclidean")
    @Description("algo.similarity.euclidean([vector1], [vector2]) " +
            "given two collection vectors, calculate similarity based on euclidean distance")
    public double euclideanSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        return 1.0D / (1 + euclideanDistance(vector1, vector2));
    }

    @UserFunction("algo.similarity.overlap")
    @Description("algo.similarity.overlap([vector1], [vector2]) " +
            "given two collection vectors, calculate overlap similarity")
    public double overlapSimilarity(@Name("vector1") List<Number> vector1, @Name("vector2") List<Number> vector2) {
        if (vector1 == null || vector2 == null) return 0;

        HashSet<Number> intersectionSet = new HashSet<>(vector1);
        intersectionSet.retainAll(vector2);
        int intersection = intersectionSet.size();

        long denominator = Math.min(vector1.size(), vector2.size());
        return denominator == 0 ? 0 : (double) intersection / denominator;
    }

}
