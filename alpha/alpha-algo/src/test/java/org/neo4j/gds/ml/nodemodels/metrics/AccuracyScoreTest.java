/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.ml.nodemodels.metrics;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.gds.ml.nodemodels.metrics.MetricsTestUtil.hugeAtomicLongArray;

class AccuracyMetricTest {

    @Test
    void shouldComputeAccuracy() {
        var predictions = hugeAtomicLongArray(new long[] {
            3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4
        });
        var targets = hugeAtomicLongArray(new long[] {
            4, 4, 5, 5, 5, 8, 9, 1, 1, 2, 2, 3, 3, 4, 5
        });
        var metric = new AccuracyMetric();
        assertThat(metric.compute(targets, predictions)).isCloseTo(7.0/15, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeAccuracyAllCorrect() {
        var predictions = hugeAtomicLongArray(new long[] {
            3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4
        });
        var targets = hugeAtomicLongArray(new long[] {
            3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4
        });
        var metric = new AccuracyMetric();
        assertThat(metric.compute(targets, predictions)).isCloseTo(1.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeAccuracyAllWrong() {
        var predictions = hugeAtomicLongArray(new long[] {
            3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4
        });
        var targets = hugeAtomicLongArray(new long[] {
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        });
        var metric = new AccuracyMetric();
        assertThat(metric.compute(targets, predictions)).isCloseTo(0.0, Offset.offset(1e-8));
    }
}
