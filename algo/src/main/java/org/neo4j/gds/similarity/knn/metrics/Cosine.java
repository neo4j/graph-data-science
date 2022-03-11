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

import org.neo4j.gds.core.utils.Intersections;

/**
 * We compute cosine similarity (normalised dot product) and turn it into a similarity metric by moving and
 * clamping -1..1 into 0..1 using linear transformation.
 */
public final class Cosine {
    private Cosine() {}

    public static double floatMetric(float[] left, float[] right) {
        var len = Math.min(left.length, right.length);
        var cosine = Intersections.cosine(left, right, len);
        return (cosine+1)/ 2;
    }

    public static double doubleMetric(double[] left, double[] right) {
        var len = Math.min(left.length, right.length);
        var cosine = Intersections.cosine(left, right, len);
        return (cosine+1)/ 2;
    }
}
