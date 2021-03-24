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

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface MetricSpecification {
    Map<String, Function<Long, Metric>> SINGLE_CLASS_METRIC_FACTORIES = Map.of(
        F1Score.NAME, F1Score::new
    );
    Pattern SINGLE_CLASS_METRIC_PATTERN = Pattern.compile("([\\p{Alnum}]+)\\([\\s]*CLASS[\\s]*=[\\s]*([\\d]+)[\\s]*\\)");

    static String composeSpecification(String metricType, long classId) {
        return formatWithLocale("%s(class=%d)", metricType, classId);
    }

    Stream<Metric> createMetrics(Collection<Long> classes);

    String asString();

    static List<MetricSpecification> parse(List<String> userSpecifications) {
        if (userSpecifications.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale("No metrics specified, we require at least one"));
        }
        var mainMetric = userSpecifications.get(0).toUpperCase(Locale.ENGLISH);
        if (SINGLE_CLASS_METRIC_FACTORIES.keySet().stream().anyMatch(metric -> metric.equals(mainMetric))) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "The primary (first) metric provided must be one of %s",
                    String.join(", ", validPrimaryMetricExpressions())
                ));
        }
        return userSpecifications.stream()
            .map(MetricSpecification::parse)
            .distinct()
            .collect(Collectors.toList());
    }

    static MetricSpecification parse(String userSpecification) {
        var upperCaseSpecification = userSpecification.toUpperCase(Locale.ENGLISH);
        for (Map.Entry<String, Function<Long, Metric>> entry : SINGLE_CLASS_METRIC_FACTORIES.entrySet()) {
            String metric = entry.getKey();
            if (metric.equals(upperCaseSpecification)) {
                return createSpecification(
                    classes -> classes
                        .stream()
                        .map(classId -> entry.getValue().apply(classId)),
                    upperCaseSpecification
                );
            }
        }
        var matcher = SINGLE_CLASS_METRIC_PATTERN.matcher(upperCaseSpecification);
        if (matcher.matches()) {
            var metricType = matcher.group(1);
            var classId = Long.parseLong(matcher.group(2));
            if (SINGLE_CLASS_METRIC_FACTORIES.containsKey(metricType)) {
                return createSpecification(
                    ignored -> Stream.of(SINGLE_CLASS_METRIC_FACTORIES
                        .get(metricType)
                        .apply(classId)),
                    composeSpecification(metricType, classId)
                );
            }
        }
        try {
            var metric = AllClassMetric.valueOf(upperCaseSpecification);
            return createSpecification(
                ignored -> Stream.of(metric),
                upperCaseSpecification
            );
        } catch (Exception e) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid metric expression `%s`. Available metrics are %s",
                userSpecification,
                String.join(", ", allValidMetricExpressions())
            ));
        }
    }

    static MetricSpecification createSpecification(
        Function<Collection<Long>, Stream<Metric>> metricFactory,
        String stringRepresentation
    ) {
        return new MetricSpecification() {
            @Override
            public Stream<Metric> createMetrics(Collection<Long> classes) {
                return metricFactory.apply(classes);
            }

            @Override
            public String asString() {
                return stringRepresentation;
            }

            @Override
            public String toString() {
                return asString();
            }

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof MetricSpecification)) {
                    return false;
                }
                return asString().equals(((MetricSpecification) obj).asString());
            }
        };
    }

    private static List<String> allValidMetricExpressions() {
        return validMetricExpressions(true);
    }

    private static List<String> validPrimaryMetricExpressions() {
        return validMetricExpressions(false);
    }

    private static List<String> validMetricExpressions(boolean includeSyntacticSugarMetrics) {
        var validExpressions = new LinkedList<String>();
        var allClassExpressions = AllClassMetric.values();
        for (AllClassMetric allClassExpression : allClassExpressions) {
            validExpressions.add(allClassExpression.name());
        }
        for (String singleClassMetric : SINGLE_CLASS_METRIC_FACTORIES.keySet()) {
            if (includeSyntacticSugarMetrics) {
                validExpressions.add(singleClassMetric);
            }
            validExpressions.add(singleClassMetric + "(class=<class value>)");
        }
        return validExpressions;
    }

    static List<String> specificationsToString(List<MetricSpecification> specifications) {
        return specifications.stream()
            .map(MetricSpecification::asString)
            .collect(Collectors.toList());
    }
}
