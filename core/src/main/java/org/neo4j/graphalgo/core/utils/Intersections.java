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
package org.neo4j.graphalgo.core.utils;

import com.carrotsearch.hppc.LongHashSet;

import java.util.Arrays;

public class Intersections {
    public static long intersection(LongHashSet targets1, LongHashSet targets2) {
        LongHashSet intersectionSet = new LongHashSet(targets1);
        intersectionSet.retainAll(targets2);
        return intersectionSet.size();
    }

    public static long intersection2(long[] targets1, long[] targets2) {
        LongHashSet intersectionSet = LongHashSet.from(targets1);
        intersectionSet.retainAll(LongHashSet.from(targets2));
        return intersectionSet.size();
    }

    // assume both are sorted
    public static long intersection3(long[] targets1, long[] targets2) {
        int len2;
        if ((len2 = targets2.length) == 0) return 0;
        int off2 = 0;
        long intersection = 0;
        for (long value1 : targets1) {
            if (value1 > targets2[off2]) {
                while (++off2 != len2 && value1 > targets2[off2]) ;
                if (off2 == len2) return intersection;
            }
            if (value1 == targets2[off2]) {
                intersection++;
                off2++;
                if (off2 == len2) return intersection;
            }
        }
        return intersection;
    }

    // idea, compute differences, when 0 then equal?
    // assume both are sorted
    public static long intersection4(long[] targets1, long[] targets2) {
        if (targets2.length == 0) return 0;
        int off2 = 0;
        long intersection = 0;
        for (int off1 = 0; off1 < targets1.length; off1++) {
            if (off2 == targets2.length) return intersection;
            long value1 = targets1[off1];

            if (value1 > targets2[off2]) {
                for (; off2 < targets2.length; off2++) {
                    if (value1 <= targets2[off2]) break;
                }
                // while (++off2 != targets2.length && value1 > targets2[off2]);
                if (off2 == targets2.length) return intersection;
            }
            if (value1 == targets2[off2]) {
                intersection++;
                off2++;
            }
        }
        return intersection;
    }

    private static int[] EMPTY = new int[0];

    public static int[] getIntersection(int[] values1, int[] values2) {
        if (values1 == null || values2 == null) return EMPTY;
        return getIntersection(values1, values1.length, values2, values2.length);
    }

    public static int[] getIntersection(int[] values1, int len1, int[] values2, int len2) {
        if (values1 == null || values2 == null) return EMPTY;
        if (len1 == 0 || len2 == 0) return EMPTY;
        int off2 = 0;
        int[] intersection = new int[Math.min(len1, len2)];
        int resIdx = 0;
        for (int i = 0; i < len1; i++) {
            int value1 = values1[i];
            if (value1 > values2[off2]) {
                while (++off2 != len2 && value1 > values2[off2]) ;
                if (off2 == len2) break;
            }
            if (value1 == values2[off2]) {
                intersection[resIdx++] = value1;
                off2++;
                if (off2 == len2) break;
            }
        }
        return resIdx < intersection.length ? Arrays.copyOf(intersection, resIdx) : intersection;
    }

    /*
    public static double sumSquareDelta(double[] vector1, double[] vector2, int len) {
        double result = 0;
        int intersection = 0;
        for (int i=0;i<len;i++) {
            double delta = vector1[i] - vector2[i];
            if (delta == 0) continue;
            result += delta * delta;
            intersection++;
        }
        return result;
    }
    */
    public static double sumSquareDeltaSkip(double[] vector1, double[] vector2, int len, double skipValue) {
        boolean skipNan = Double.isNaN(skipValue);

        double result = 0;
        double comparisons = 0;
        for (int i = 0; i < len; i++) {
            double weight1 = vector1[i];
            if (shouldSkip(weight1, skipValue, skipNan)) continue;

            double weight2 = vector2[i];
            if (shouldSkip(weight2, skipValue, skipNan)) continue;

            double delta = weight1 - weight2;
            result += delta * delta;
            comparisons++;
        }

        return comparisons == 0 ? Double.NaN : result;
    }

    public static double sumSquareDelta(double[] vector1, double[] vector2, int len) {
        double result = 0;
        for (int i = 0; i < len; i++) {
            double delta = vector1[i] - vector2[i];
            result += delta * delta;
        }
        return result;
    }

    public static double[] sumSquareDeltas(double[] vector1, double[][] vector2, int len) {
        int vectors = vector2.length;
        double[] result = new double[vectors];
        for (int i = 0; i < len; i++) {
            double v1 = vector1[i];
            for (int j = 0; j < vectors; j++) {
                result[j] += (v1 - vector2[j][i]) * (v1 - vector2[j][i]);
            }
        }
        return result;
    }

    public static double cosineSquare(double[] vector1, double[] vector2, int len) {
        double dotProduct = 0D;
        double xLength = 0D;
        double yLength = 0D;
        for (int i = 0; i < len; i++) {
            double weight1 = vector1[i];
            double weight2 = vector2[i];
            dotProduct += weight1 * weight2;
            xLength += weight1 * weight1;
            yLength += weight2 * weight2;
        }
        if (xLength == 0D || yLength == 0D) return 0D;
        return dotProduct * dotProduct / xLength / yLength;
    }

    public static double cosineSquareSkip(double[] vector1, double[] vector2, int len, double skipValue) {
        boolean skipNan = Double.isNaN(skipValue);

        double dotProduct = 0D;
        double xLength = 0D;
        double yLength = 0D;
        for (int i = 0; i < len; i++) {
            double weight1 = vector1[i];
            if (shouldSkip(weight1, skipValue, skipNan)) continue;
            double weight2 = vector2[i];
            if (shouldSkip(weight2, skipValue, skipNan)) continue;

            dotProduct += weight1 * weight2;
            xLength += weight1 * weight1;
            yLength += weight2 * weight2;
        }

        if (xLength == 0D || yLength == 0D) return 0D;
        return dotProduct * dotProduct / xLength / yLength;
    }

    public static double pearson(double[] vector1, double[] vector2, int len) {
        double vector1Sum = 0.0;
        double vector2Sum = 0.0;
        for (int i = 0; i < len; i++) {
            vector1Sum += vector1[i];
            vector2Sum += vector2[i];
        }

        double vector1Mean = vector1Sum / len;
        double vector2Mean = vector2Sum / len;

        double dotProductMinusMean = 0D;
        double xLength = 0D;
        double yLength = 0D;
        for (int i = 0; i < len; i++) {
            double vector1Delta = vector1[i] - vector1Mean;
            double vector2Delta = vector2[i] - vector2Mean;

            dotProductMinusMean += (vector1Delta * vector2Delta);
            xLength += vector1Delta * vector1Delta;
            yLength += vector2Delta * vector2Delta;
        }

        double result = dotProductMinusMean / Math.sqrt(xLength * yLength);
        return Double.isNaN(result) ? 0 : result;
    }

    public static double pearsonSkip(double[] vector1, double[] vector2, int len, double skipValue) {
        boolean skipNan = Double.isNaN(skipValue);

        double vector1Sum = 0.0;
        int vector1Count = 0;
        double vector2Sum = 0.0;
        int vector2Count = 0;
        for (int i = 0; i < len; i++) {
            double weight1 = vector1[i];
            double weight2 = vector2[i];

            if(!shouldSkip(weight1, skipValue, skipNan)) {
                vector1Sum += weight1;
                vector1Count++;
            }

            if (!shouldSkip(weight2, skipValue, skipNan)) {
                vector2Sum += weight2;
                vector2Count++;
            }
        }

        double vector1Mean = vector1Sum / vector1Count;
        double vector2Mean = vector2Sum / vector2Count;

        double dotProductMinusMean = 0D;
        double xLength = 0D;
        double yLength = 0D;
        for (int i = 0; i < len; i++) {
            double weight1 = vector1[i];
            if (shouldSkip(weight1, skipValue, skipNan)) continue;

            double weight2 = vector2[i];
            if (shouldSkip(weight2, skipValue, skipNan)) continue;

            double vector1Delta = weight1 - vector1Mean;
            double vector2Delta = weight2 - vector2Mean;

            dotProductMinusMean += (vector1Delta * vector2Delta);
            xLength += vector1Delta * vector1Delta;
            yLength += vector2Delta * vector2Delta;
        }

        double result = dotProductMinusMean / Math.sqrt(xLength * yLength);
        return Double.isNaN(result) ? 0 : result;
    }

    public static boolean shouldSkip(double propertyValue, double skipValue, boolean skipNan) {
        return propertyValue == skipValue || (skipNan && Double.isNaN(propertyValue));
    }

    public static double cosine(double[] vector1, double[] vector2, int len) {
        double dotProduct = 0D;
        double xLength = 0D;
        double yLength = 0D;
        for (int i = 0; i < len; i++) {
            double weight1 = vector1[i];
            // if (weight1 == 0d) continue;
            double weight2 = vector2[i];
            // if (weight2 == 0d) continue;

            dotProduct += weight1 * weight2;
            xLength += weight1 * weight1;
            yLength += weight2 * weight2;
        }

        return dotProduct / Math.sqrt(xLength * yLength);
    }


    public static double[] cosines(double[] vector1, double[][] vector2, int len) {
        int vectors = vector2.length;
        double[] dotProduct = new double[vectors];
        double xLength = 0D;
        double[] yLength = new double[vectors];
        for (int i = 0; i < len; i++) {
            double weight1 = vector1[i];
            // todo does this influence simd?
            // if (weight1 == 0d) continue;

            xLength += weight1 * weight1;
            for (int j = 0; j < vectors; j++) {
                double weight2 = vector2[j][i];
                // todo does this influence simd?
                // if (weight2 == 0d) continue;
                dotProduct[j] += weight1 * weight2;
                yLength[j] += weight2 * weight2;
            }
        }
        xLength = Math.sqrt(xLength);
        for (int j = 0; j < vectors; j++) {
            dotProduct[j] /= (xLength * Math.sqrt(yLength[j]));
        }
        return dotProduct;
    }
}
