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

public final class Euclidean {
    private Euclidean() {}

    public static double floatMetric(float[] left, float[] right) {
        var len = Math.min(left.length, right.length);
        var result = 0D;
        for (int i = 0; i < len; i++) {
            double delta = left[i] - right[i];
            result += delta * delta;
        }
        return 1.0 / (1.0 + result);
    }

    public static double doubleMetric(double[] left, double[] right) {
        var len = Math.min(left.length, right.length);
        var result = 0D;
        for (int i = 0; i < len; i++) {
            double delta = left[i] - right[i];
            result += delta * delta;
        }
        return 1.0 / (1.0 + result);
    }
}
