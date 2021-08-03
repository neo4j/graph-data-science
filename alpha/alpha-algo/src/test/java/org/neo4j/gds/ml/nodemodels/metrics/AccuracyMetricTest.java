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
package org.neo4j.gds.ml.nodemodels.metrics;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.openjdk.jol.util.Multiset;

import static org.assertj.core.api.Assertions.assertThat;

class AccuracyMetricTest {
    @Test
    void shouldComputeAccuracy() {
        var predictions = HugeLongArray.of(3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4);
        var targets = HugeLongArray.of(4, 4, 5, 5, 5, 8, 9, 1, 1, 2, 2, 3, 3, 4, 5);
        var classCounts = new Multiset<Long>();
        for (long target : targets.toArray()) {
            classCounts.add(target);
        }

        var metric = AllClassMetric.ACCURACY;
        assertThat(metric.compute(targets, predictions, classCounts)).isCloseTo(7.0 / 15, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeAccuracyWhenAllPredictionsAreCorrect() {
        var predictions = HugeLongArray.of(3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4);
        var targets = HugeLongArray.of(3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4);
        var classCounts = new Multiset<Long>();
        for (long target : targets.toArray()) {
            classCounts.add(target);
        }

        var metric = AllClassMetric.ACCURACY;
        assertThat(metric.compute(targets, predictions, classCounts)).isCloseTo(1.0, Offset.offset(1e-8));
    }

    @Test
    void shouldComputeAccuracyWhenAllPredictionsAreWrong() {
        var predictions = HugeLongArray.of(3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4);
        var targets = HugeLongArray.of(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        var classCounts = new Multiset<Long>();
        for (long target : targets.toArray()) {
            classCounts.add(target);
        }

        var metric = AllClassMetric.ACCURACY;
        assertThat(metric.compute(targets, predictions, classCounts)).isCloseTo(0.0, Offset.offset(1e-8));
    }
}
