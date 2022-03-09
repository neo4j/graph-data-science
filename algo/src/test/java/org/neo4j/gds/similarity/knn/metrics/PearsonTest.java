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

import static org.junit.jupiter.api.Assertions.assertEquals;

class PearsonTest {
    @Test
    void shouldComputePearsonCorrelation() {
        // numbers from https://www.sciencedirect.com/topics/computer-science/pearson-correlation
        double pearsonCorrelation = Pearson.doubleMetric(
            new double[]{1, 4, 6, 9, 15, 55, 62, -5},
            new double[]{-2, -8, -9, -12, -80, 14, 15, 2}
        );

        assertEquals(0.6644688329363109, pearsonCorrelation);
    }

    @Test
    void shouldComputePearsonAgain() {
        // numbers from https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.pearsonr.html
        double pearsonCorrelation = Pearson.doubleMetric(
            new double[]{1, 2, 3, 4, 5},
            new double[]{10, 9, 2.5, 6, 4}
        );

        assertEquals(0.12869467138374713, pearsonCorrelation);
    }

    @Test
    void shouldComputePearsonCorrelationForFloats() {
        // numbers from https://www.sciencedirect.com/topics/computer-science/pearson-correlation
        double pearsonCorrelation = Pearson.floatMetric(
            new float[]{1, 4, 6, 9, 15, 55, 62, -5},
            new float[]{-2, -8, -9, -12, -80, 14, 15, 2}
        );

        assertEquals(0.6644688329363109, pearsonCorrelation);
    }

    @Test
    void shouldComputePearsonAgainForFloats() {
        // numbers from https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.pearsonr.html
        double pearsonCorrelation = Pearson.floatMetric(
            new float[]{1, 2, 3, 4, 5},
            new float[]{10, 9, 2.5f, 6, 4}
        );

        assertEquals(0.12869467138374713, pearsonCorrelation);
    }

    @Test
    void shouldIgnoreLongTail() {
        // numbers from https://www.sciencedirect.com/topics/computer-science/pearson-correlation
        double pearsonCorrelation = Pearson.doubleMetric(
            new double[]{1, 4, 6, 9, 15, 55, 62, -5},
            new double[]{-2, -8, -9, -12, -80, 14, 15, 2, 42, 87, 23, 117}
        );

        assertEquals(0.6644688329363109, pearsonCorrelation);
    }
}
