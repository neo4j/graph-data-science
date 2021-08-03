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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.openjdk.jol.util.Multiset;

import static org.assertj.core.api.Assertions.assertThat;

class F1WeightedTest {

    private HugeLongArray targets;
    private HugeLongArray predictions;
    private Multiset<Long> classCounts;

    @BeforeEach
    void setup() {
        predictions = HugeLongArray.of(
            3, 4, 6, 6, 7, 9, 8, 1, 1, 2, 3, 3, 3, 4, 4
        );
        targets = HugeLongArray.of(
            4, 4, 5, 5, 5, 8, 9, 1, 1, 2, 2, 3, 3, 4, 5
        );
        classCounts = new Multiset<>();
        for (long target : targets.toArray()) {
            classCounts.add(target, 2L);
        }
    }

    @Test
    void shouldComputeF1AllCorrectMultiple() {
        var metric = AllClassMetric.F1_WEIGHTED;
        var totalF1 = 2 * 1.0 + 2 * 2.0/3.0 + 2 * 2.0/3.0 + 3 * 2.0/3.0;
        var totalExamples = predictions.size();
        assertThat(metric.compute(targets, predictions, classCounts))
            .isCloseTo(totalF1 / totalExamples, Offset.offset(1e-8));
    }
}
