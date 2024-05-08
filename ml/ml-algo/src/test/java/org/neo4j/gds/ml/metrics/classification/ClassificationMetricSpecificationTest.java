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
package org.neo4j.gds.ml.metrics.classification;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.collections.LongMultiSet;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.mem.MemoryRange;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;

import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class ClassificationMetricSpecificationTest {
    @Test
    void shouldCreateF1Metric() {
        var metricSpecification = ClassificationMetricSpecification.Parser.parse(List.of("F1(clAss =  42 )")).get(0);

        var classCounts = new LongMultiSet();
        classCounts.add(1337L);
        var metric = metricSpecification.createMetrics(LocalIdMap.of(1337L), classCounts).findFirst().orElseThrow();
        assertThat(metric.getClass()).isEqualTo(F1Score.class);
        assertThat(metric.toString()).isEqualTo("F1_class_42");
        assertThat(metric.name()).isEqualTo("F1(class=42)");
        assertThat(metricSpecification.toString()).isEqualTo("F1(class=42)");
    }

    @Test
    void shouldCreateAccuracyMetric() {
        var metricSpecification = ClassificationMetricSpecification.Parser.parse(List.of("Accuracy")).get(0);

        var classCounts = new LongMultiSet();
        classCounts.add(1337L);
        var metric = metricSpecification.createMetrics(LocalIdMap.of(1337L), classCounts).findFirst().orElseThrow();
        assertThat(metric.toString()).isEqualTo("ACCURACY");
        assertThat(metricSpecification.toString()).isEqualTo("ACCURACY");
        assertThat(metric).isEqualTo(new GlobalAccuracy());
    }

    @Test
    void shouldCreateGlobalAccuracyMetric() {
        var metricSpecification = ClassificationMetricSpecification.Parser.parse(List.of("Accuracy(class=1337)")).get(0);

        var classCounts = new LongMultiSet();
        classCounts.add(1337L);
        var metric = metricSpecification.createMetrics(LocalIdMap.of(1337L), classCounts).findFirst().orElseThrow();
        assertThat(metricSpecification.toString()).isEqualTo("ACCURACY(class=1337)");
        assertThat(metric).isEqualTo(new Accuracy(1337, 0));
    }

    @Test
    void shouldCreateF1WeightedMetric() {
        var metricSpecification = ClassificationMetricSpecification.Parser.parse(List.of("F1_WeIGhTED")).get(0);

        var classCounts = new LongMultiSet();
        classCounts.add(1337L);
        var metric = metricSpecification.createMetrics(LocalIdMap.of(1337L), classCounts).findFirst().orElseThrow();
        assertThat(metric.toString()).isEqualTo("F1_WEIGHTED");
        assertThat(metric).isEqualTo(new F1Weighted(LocalIdMap.of(1337L), classCounts));
    }

    @Test
    void shouldCreateF1MacroMetric() {
        var metricSpecification = ClassificationMetricSpecification.Parser.parse(List.of("F1_maCRo")).get(0);

        var classCounts = new LongMultiSet();
        classCounts.add(1337L);
        var metric = metricSpecification.createMetrics(LocalIdMap.of(1337L), classCounts).findFirst().orElseThrow();
        assertThat(metric.toString()).isEqualTo("F1_MACRO");
        assertThat(metric).isEqualTo(new F1Macro(LocalIdMap.of(1337L)));
    }

    @Test
    void shouldCreateOOBEMetric() {
        var metricSpecification = ClassificationMetricSpecification.Parser.parse(List.of("OuT_Of_BAG_ErROR")).get(0);

        var classCounts = new LongMultiSet();
        classCounts.add(1337L);
        var metric = metricSpecification.createMetrics(LocalIdMap.of(1337L), classCounts).findFirst().orElseThrow();
        assertThat(metric.getClass()).isEqualTo(OutOfBagError.class);
        assertThat(metric.toString()).isEqualTo("OUT_OF_BAG_ERROR");
        assertThat(metric.name()).isEqualTo("OUT_OF_BAG_ERROR");
        assertThat(metricSpecification.toString()).isEqualTo("OUT_OF_BAG_ERROR");
    }

    @Test
    void shouldParseSyntacticSugar() {
        var metricSpecification = ClassificationMetricSpecification.Parser.parse(List.of("Accuracy", "F1(class=*)")).get(1);

        var classCounts = new LongMultiSet();
        classCounts.add(-1337L);
        classCounts.add(42L);
        var metrics = metricSpecification.createMetrics(LocalIdMap.of(42L, -1337L), classCounts).collect(Collectors.toList());
        assertThat(metrics).containsExactlyInAnyOrder(new F1Score(42,0), new F1Score(-1337,0));

    }

    @ParameterizedTest
    @MethodSource("invalidSingleClassSpecifications")
    void shouldFailOnInvalidSingleClassSpecifications(String metric) {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> ClassificationMetricSpecification.Parser.parse(metric))
            .withMessageContaining(
                "Invalid metric expression"
            );
    }

    @Test
    void shouldFailOnMultipleInvalidSingleClassSpecifications() {
        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> ClassificationMetricSpecification.Parser.parse(invalidSingleClassSpecifications()))
            .withMessageContaining(
                "Invalid metric expressions"
            );
    }

    @Test
    void shouldEstimateMemoryUsage() {
        var nodeCount = 1_000_000_000;
        var numberOfClasses = 1000;
        var actual = ClassificationMetricSpecification.memoryEstimation(numberOfClasses).
            estimate(GraphDimensions.of(nodeCount), new Concurrency(1))
            .memoryUsage();
        var expected = MemoryRange.of(1, numberOfClasses).times(24);
        assertThat(actual).isEqualTo(expected);
    }

    private static List<String> invalidSingleClassSpecifications() {
        return List.of("F 1 ( class=2 3 4)", "F 1 ( class=3)", "f1(c las s = 01 0 30 2)", "JAMESBOND(class=0)", "F1(class=$)");
    }
}
