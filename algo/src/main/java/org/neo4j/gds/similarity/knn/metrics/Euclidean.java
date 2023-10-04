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
package org.neo4j.gds.similarity.knn.metrics;

import java.util.function.IntToDoubleFunction;

/**
 * Here we calculate Euclidean similarity metrics using Euclidean dictance as described in e.g.
 * https://en.wikipedia.org/wiki/Euclidean_distance
 *
 * We specifically calculate the Euclidean squared distance for the overlap of the arrays, potentially ignoring the
 * tail of one of them.
 *
 * We then normalise this squared distance in order to clamp the number into the range (0,1] so that the metric can be
 * used for comparisons up stream.
 */
public final class Euclidean {
    private Euclidean() {}

    public static double floatMetric(float[] left, float[] right) {
        return compute(
            Math.min(left.length, right.length),
            i -> left[i],
            i -> right[i]
        );
    }

    public static double doubleMetric(double[] left, double[] right) {
        return compute(
            Math.min(left.length, right.length),
            i -> left[i],
            i -> right[i]
        );
    }

    private static double compute(int len, IntToDoubleFunction left, IntToDoubleFunction right) {
        var result = 0D;
        for (int i = 0; i < len; i++) {
            double delta = left.applyAsDouble(i) - right.applyAsDouble(i);
            result += delta * delta;
        }

        return 1.0 / (1.0 + Math.sqrt(result));
    }
}
