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

import org.junit.jupiter.api.Test;

import static java.lang.Math.sqrt;
import static org.junit.jupiter.api.Assertions.assertEquals;

class CosineTest {
    @Test
    void geometryIsFun() {
        assertEquals(1.0, Cosine.doubleMetric(new double[]{1, 0, 0}, new double[]{1, 0, 0}), "identical vectors are perfectly similar");
        assertEquals(0.5, Cosine.doubleMetric(new double[]{1, 0, 0}, new double[]{0, 1, 0}), "perpendicular vectors");
        assertEquals(0.0, Cosine.doubleMetric(new double[]{1, 0, 0}, new double[]{-1, 0, 0}), "opposite vectors");

        /*
         * 45 degree angle v and Pythagoras:
         *   cos(v)^2 + sin(v)^2 = 1^2
         *   => 2cos(v)^2 = 1
         *   => cos(v) = 1/ sqrt(2)
         *   => sqrt(2)/ sqrt(2)^2
         *   => sqrt(2)/ 2
         *
         *   Then we do (sqrt(2)/ 2 + 1)/ 2 to normalise the score
         *   => 2(sqrt(2)/ 2 + 1)/ 4
         *   => (2sqrt(2)/ 2 + 2)/ 4
         *   => (sqrt(2) + 2)/ 4
         */
        assertEquals((sqrt(2)+2)/ 4, Cosine.doubleMetric(new double[]{1, 0, 0}, new double[]{1, 1, 0}), 0.0000000000001, "45 degrees");
    }
}
