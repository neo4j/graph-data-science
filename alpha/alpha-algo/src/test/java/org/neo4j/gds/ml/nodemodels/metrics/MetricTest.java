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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class MetricTest {

    @Test
    void shouldCreateF1Metric() {
        var metric = Metric.resolveMetric("F1(class=42)");
        assertThat(metric.getClass()).isEqualTo(F1Score.class);
        assertThat(((F1Score)metric).positiveTarget()).isEqualTo(42L);
    }

    @Test
    void shouldCreateAccuracyMetric() {
        var metric = Metric.resolveMetric("Accuracy");
        assertThat(metric).isEqualTo(AllClassMetric.ACCURACY);
    }

    @Test
    void shouldCreateF1WeightedMetric() {
        var metric = Metric.resolveMetric("F1_WeIGhTED");
        assertThat(metric).isEqualTo(AllClassMetric.F1_WEIGHTED);
    }

    @Test
    void shouldCreateF1MacroMetric() {
        var metric = Metric.resolveMetric("F1_maCRo");
        assertThat(metric).isEqualTo(AllClassMetric.F1_MACRO);
    }
}
