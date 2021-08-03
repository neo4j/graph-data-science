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

import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryRange;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOf;

public interface MetricSpecification {
    SortedMap<String, Function<Long, Metric>> SINGLE_CLASS_METRIC_FACTORIES = new TreeMap<>(Map.of(
        F1Score.NAME, F1Score::new,
        Precision.NAME, Precision::new,
        Recall.NAME, Recall::new,
        Accuracy.NAME, Accuracy::new
    ));
    String NUMBER_OR_STAR = "((?:-?[\\d]+)|(?:\\*))";
    String VALID_SINGLE_CLASS_METRICS = String.join("|", SINGLE_CLASS_METRIC_FACTORIES.keySet());
    Pattern SINGLE_CLASS_METRIC_PATTERN = Pattern.compile(
        "(" + VALID_SINGLE_CLASS_METRICS + ")" +
        "\\([\\s]*CLASS[\\s]*=[\\s]*" + NUMBER_OR_STAR + "[\\s]*\\)");

    static MemoryEstimation memoryEstimation(int numberOfClasses) {
        return MemoryEstimations.builder()
            .rangePerNode("metrics", __ -> {
                var sizeOfRepresentativeMetric = sizeOf(new F1Score(1));
                return MemoryRange.of(1 * sizeOfRepresentativeMetric, numberOfClasses * sizeOfRepresentativeMetric);
            })
            .build();
    }

    static String composeSpecification(String metricType, String classId) {
        return formatWithLocale("%s(class=%s)", metricType, classId);
    }

    Stream<Metric> createMetrics(Collection<Long> classes);

    String asString();

    static List<MetricSpecification> parse(List<String> userSpecifications) {
        if (userSpecifications.isEmpty()) {
            throw new IllegalArgumentException(formatWithLocale("No metrics specified, we require at least one"));
        }
        var mainMetric = userSpecifications.get(0).toUpperCase(Locale.ENGLISH);
        var errors = new ArrayList<String>();
        if (mainMetric.contains("*")) {
                errors.add(formatWithLocale(
                    "The primary (first) metric provided must be one of %s.",
                    String.join(", ", validPrimaryMetricExpressions())
                ));
        }
        List<String> badSpecifications = userSpecifications
            .stream()
            .filter(MetricSpecification::invalidSpecification)
            .collect(Collectors.toList());
        if (!badSpecifications.isEmpty()) {
            errors.add(errorMessage(badSpecifications));
        }
        if (!errors.isEmpty()) {
            throw new IllegalArgumentException(String.join(" ", errors));
        }
        return userSpecifications.stream()
            .map(MetricSpecification::parse)
            .distinct()
            .collect(Collectors.toList());
    }

    static MetricSpecification parse(String userSpecification) {
        var upperCaseSpecification = toUpperCaseWithLocale(userSpecification);
        var matcher = SINGLE_CLASS_METRIC_PATTERN.matcher(upperCaseSpecification);
        if (!matcher.matches()) {
            try {
                var metric = AllClassMetric.valueOf(upperCaseSpecification);
                return createSpecification(
                    ignored -> Stream.of(metric),
                    upperCaseSpecification
                );
            } catch (Exception e) {
                failSingleSpecification(userSpecification);
            }
        }
        var metricType = matcher.group(1);
        if (matcher.group(2).equals("*")) {
            for (Map.Entry<String, Function<Long, Metric>> entry : SINGLE_CLASS_METRIC_FACTORIES.entrySet()) {
                String metric = entry.getKey();
                if (metric.equals(metricType)) {
                    return createSpecification(
                        classes -> classes
                            .stream()
                            .map(classId -> entry.getValue().apply(classId)),
                        composeSpecification(metricType, "*")
                    );
                }
            }
        }
        var classId = Long.parseLong(matcher.group(2));
        return createSpecification(
            ignored -> Stream.of(SINGLE_CLASS_METRIC_FACTORIES
                .get(metricType)
                .apply(classId)),
            composeSpecification(metricType, String.valueOf(classId))
        );
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
                validExpressions.add(singleClassMetric + "(class=*)");
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

    static void failSingleSpecification(String userSpecification) {
        throw new IllegalArgumentException(errorMessage(List.of(userSpecification)));
    }

    static String errorMessage(List<String> specifications) {
        return formatWithLocale(
            "Invalid metric expression%s %s. Available metrics are %s (case insensitive and space allowed between brackets).",
            specifications.size() == 1 ? "" : "s",
            specifications.stream().map(s -> "`" + s + "`").collect(Collectors.joining(", ")),
            String.join(", ", allValidMetricExpressions())
        );
    }

    static boolean invalidSpecification(String userSpecification) {
        var upperCaseSpecification = userSpecification.toUpperCase(Locale.ENGLISH);
        var matcher = SINGLE_CLASS_METRIC_PATTERN.matcher(upperCaseSpecification);
        if (matcher.matches()) return false;
        return Arrays
            .stream(AllClassMetric.values())
            .map(AllClassMetric::name)
            .noneMatch(upperCaseSpecification::equals);
    }
}
