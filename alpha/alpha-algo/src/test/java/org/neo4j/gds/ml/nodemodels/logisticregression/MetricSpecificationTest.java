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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.nodemodels.metrics.AllClassMetric;
import org.neo4j.gds.ml.nodemodels.metrics.F1Score;
import org.neo4j.gds.ml.nodemodels.metrics.Metric;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class MetricSpecificationTest {
    @Test
    void shouldCreateF1Metric() {
        var metricSpecification = MetricSpecification.parse(List.of("F1(class=42)")).get(0);
        var metric = metricSpecification.createMetrics(List.of(1337L)).findFirst().get();
        assertThat(metric.getClass()).isEqualTo(F1Score.class);
        assertThat(metric.toString()).isEqualTo("F1_class_42");
        assertThat(metricSpecification.asString()).isEqualTo("F1(class=42)");
    }

    @Test
    void shouldCreateAccuracyMetric() {
        var metricSpecification = MetricSpecification.parse(List.of("Accuracy")).get(0);
        Metric metric = metricSpecification.createMetrics(List.of(1337L)).findFirst().get();
        assertThat(metric.toString()).isEqualTo("ACCURACY");
        assertThat(metricSpecification.asString()).isEqualTo("ACCURACY");
        assertThat(metric).isEqualTo(AllClassMetric.ACCURACY);
    }

    @Test
    void shouldCreateF1WeightedMetric() {
        var metricSpecification = MetricSpecification.parse(List.of("F1_WeIGhTED")).get(0);
        Metric metric = metricSpecification.createMetrics(List.of(1337L)).findFirst().get();
        assertThat(metric.toString()).isEqualTo("F1_WEIGHTED");
        assertThat(metric).isEqualTo(AllClassMetric.F1_WEIGHTED);
    }

    @Test
    void shouldCreateF1MacroMetric() {
        var metricSpecification = MetricSpecification.parse(List.of("F1_maCRo")).get(0);
        Metric metric = metricSpecification.createMetrics(List.of(1337L)).findFirst().get();
        assertThat(metric.toString()).isEqualTo("F1_MACRO");
        assertThat(metric).isEqualTo(AllClassMetric.F1_MACRO);
    }

    @Test
    void shouldParseSyntacticSugar() {
        var metricSpecification = MetricSpecification.parse(List.of("Accuracy", "F1")).get(1);
        List<Metric> metrics = metricSpecification.createMetrics(List.of(42L, 1337L)).collect(Collectors.toList());
        assertThat(metrics.get(0).getClass()).isEqualTo(F1Score.class);
        assertThat(metrics.get(0).toString()).isEqualTo("F1_class_42");
        assertThat(metrics.get(1).getClass()).isEqualTo(F1Score.class);
        assertThat(metrics.get(1).toString()).isEqualTo("F1_class_1337");
    }

    public static List<String> allValidMetricSpecifications() {
        var validExpressions = new LinkedList<String>();
        var allClassExpressions = AllClassMetric.values();
        for (AllClassMetric allClassExpression : allClassExpressions) {
            validExpressions.add(allClassExpression.name());
        }
        for (String singleClassMetric : MetricSpecification.SINGLE_CLASS_METRIC_FACTORIES.keySet()) {
            validExpressions.add(singleClassMetric);
            validExpressions.add(singleClassMetric + "(class=0)");
        }
        return validExpressions;
    }
}
