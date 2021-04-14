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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.core.GraphDimensions;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class MetricSpecificationTest {
    @Test
    void shouldCreateF1Metric() {
        var metricSpecification = MetricSpecification.parse(List.of("F1(clAss =  42 )")).get(0);
        var metric = metricSpecification.createMetrics(List.of(1337L)).findFirst().get();
        assertThat(metric.getClass()).isEqualTo(F1Score.class);
        assertThat(metric.toString()).isEqualTo("F1_class_42");
        assertThat(metric.name()).isEqualTo("F1(class=42)");
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
        var metricSpecification = MetricSpecification.parse(List.of("Accuracy", "F1(class=*)")).get(1);
        List<Metric> metrics = metricSpecification.createMetrics(List.of(42L, -1337L)).collect(Collectors.toList());
        assertThat(metrics.get(0).getClass()).isEqualTo(F1Score.class);
        assertThat(metrics.get(0).toString()).isEqualTo("F1_class_42");
        assertThat(metrics.get(0).name()).isEqualTo("F1(class=42)");
        assertThat(metrics.get(1).getClass()).isEqualTo(F1Score.class);
        assertThat(metrics.get(1).toString()).isEqualTo("F1_class_-1337");
        assertThat(metrics.get(1).name()).isEqualTo("F1(class=-1337)");
    }

    @ParameterizedTest
    @MethodSource("invalidSingleClassSpecifications")
    void shouldFailOnInvalidSingleClassSpecifications(String metric) {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> MetricSpecification.parse(metric))
            .withMessageContaining(
                "Invalid metric expression"
            );
    }

    @Test
    void shouldFailOnMultipleInvalidSingleClassSpecifications() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> MetricSpecification.parse(invalidSingleClassSpecifications()))
            .withMessageContaining(
                "Invalid metric expressions"
            );
    }

    @Test
    void shouldEstimateMemoryUsage() {
        var nodeCount = 1000;
        var actual = MetricSpecification.memoryEstimation().estimate(GraphDimensions.of(nodeCount), 1).memoryUsage();
        var expected = MemoryRange.of(24 * 2, 24 * nodeCount);
        assertThat(actual).isEqualTo(expected);
    }

    private static List<String> invalidSingleClassSpecifications() {
        return List.of("F 1 ( class=2 3 4)", "F 1 ( class=3)", "f1(c las s = 01 0 30 2)", "JAMESBOND(class=0)", "F1(class=$)");
    }

    public static List<String> allValidMetricSpecifications() {
        var validExpressions = new LinkedList<String>();
        var allClassExpressions = AllClassMetric.values();
        for (AllClassMetric allClassExpression : allClassExpressions) {
            validExpressions.add(allClassExpression.name());
        }
        for (String singleClassMetric : MetricSpecification.SINGLE_CLASS_METRIC_FACTORIES.keySet()) {
            validExpressions.add(singleClassMetric + "(class=*)");
            validExpressions.add(singleClassMetric + "(class=0)");
        }
        return validExpressions;
    }
}
