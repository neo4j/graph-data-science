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

/**
 * We compute the Hamming Distance,
 * (https://en.wikipedia.org/wiki/Hamming_distance) and turn it into
 * a similarity metric by clamping into 0..1 range using a linear
 * transformation.
 */
public final class HammingDistance {
    private HammingDistance() {}

    public static double longMetric(long left, long right) {
        return normalizeBitCount(
                Long.bitCount(left ^ right)
        );
    }

    /**
     * We use unity-based normalization to scale the bit
     * count to the [0-1] range:
     * y = (x_i - min(x)) / (max(x) - min(x)) See
     * https://stats.stackexchange.com/a/70807 for example.
     * In our case, min(x) = 0 since you cannot have a negative
     * bit count, and max(x) = 64 since in Java, a long is
     * 64 bits in size.
     *
     * We then subtract the normalized range from 1.0 to map
     * 1.0 as most similar, and 0.0 as least similar.
     */
    private static double normalizeBitCount(long bitCount) {
        return 1.0 - (bitCount / 64.0);
    }
}
