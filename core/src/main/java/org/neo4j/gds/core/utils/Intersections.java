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
package org.neo4j.gds.core.utils;

import com.carrotsearch.hppc.LongHashSet;

public final class Intersections {

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

    public static long intersection3(long[] targets1, long[] targets2, int len1, int len2) {
        assert len1 <= targets1.length;
        assert len2 <= targets2.length;
        if (len2 == 0) return 0;
        int off2 = 0;
        long intersection = 0;
        int idx1 = 0;
        while (idx1 < len1) {
            var value1 = targets1[idx1];
            if (value1 > targets2[off2]) {
                while (++off2 != len2 && value1 > targets2[off2]) ;
                if (off2 == len2) return intersection;
            }
            if (value1 == targets2[off2]) {
                intersection++;
                off2++;
                if (off2 == len2) return intersection;
            }
            idx1++;
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

    public static double sumSquareDelta(double[] vector1, double[] vector2, int len) {
        double result = 0;
        for (int i = 0; i < len; i++) {
            double delta = vector1[i] - vector2[i];
            result += delta * delta;
        }
        return result;
    }

    public static float sumSquareDelta(float[] vector1, float[] vector2, int len) {
        float result = 0;
        for (int i = 0; i < len; i++) {
            float delta = vector1[i] - vector2[i];
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

    public static float cosine(float[] vector1, float[] vector2, int len) {
        float dotProduct = 0F;
        float xLength = 0F;
        float yLength = 0F;
        for (int i = 0; i < len; i++) {
            float weight1 = vector1[i];
            // if (weight1 == 0d) continue;
            float weight2 = vector2[i];
            // if (weight2 == 0d) continue;

            dotProduct += weight1 * weight2;
            xLength += weight1 * weight1;
            yLength += weight2 * weight2;
        }

        return (float) (dotProduct / Math.sqrt(xLength * yLength));
    }
}
