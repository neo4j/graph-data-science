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

import org.eclipse.collections.api.block.function.primitive.LongIntToObjectFunction;
import org.intellij.lang.annotations.RegExp;
import org.neo4j.gds.collections.LongMultiSet;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryRange;
import org.neo4j.gds.ml.core.subgraph.LocalIdMap;
import org.neo4j.gds.ml.metrics.Metric;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.ml.metrics.classification.OutOfBagError.OUT_OF_BAG_ERROR;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public final class ClassificationMetricSpecification {

    private final String stringRepresentation;
    private final BiFunction<LocalIdMap, LongMultiSet, Stream<Metric>> metricFactory;

    private ClassificationMetricSpecification(
        String stringRepresentation,
        BiFunction<LocalIdMap, LongMultiSet, Stream<Metric>> metricFactory
    ) {
        this.stringRepresentation = stringRepresentation;
        this.metricFactory = metricFactory;
    }

    private static ClassificationMetricSpecification createSpecification(
        BiFunction<LocalIdMap, LongMultiSet, Stream<Metric>> metricFactory,
        String stringRepresentation
    ) {
        return new ClassificationMetricSpecification(stringRepresentation, metricFactory);
    }

    public Stream<Metric> createMetrics(LocalIdMap classIdMap, LongMultiSet classCounts) {
        return metricFactory.apply(classIdMap, classCounts);
    }

    @Override
    public String toString() {
        return stringRepresentation;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ClassificationMetricSpecification)) {
            return false;
        }
        return toString().equals(obj.toString());
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    public static MemoryEstimation memoryEstimation(int numberOfClasses) {
        return MemoryEstimations.builder()
            .rangePerNode("metrics", __ -> {
                long sizeOfRepresentativeMetric = 8 + 8 + 8;
                return MemoryRange.of(sizeOfRepresentativeMetric, numberOfClasses * sizeOfRepresentativeMetric);
            })
            .build();
    }

    public static List<String> specificationsToString(List<ClassificationMetricSpecification> specifications) {
        return specifications.stream()
            .map(ClassificationMetricSpecification::toString)
            .collect(Collectors.toList());
    }

    public static final class Parser {

        private static final List<String> MODEL_SPECIFIC_METRICS = List.of(OUT_OF_BAG_ERROR.name());
        private static final Map<String, LongIntToObjectFunction<ClassificationMetric>> SINGLE_CLASS_METRIC_FACTORIES = Map.of(
                F1Score.NAME, F1Score::new,
                Precision.NAME, Precision::new,
                Recall.NAME, Recall::new,
                Accuracy.NAME, Accuracy::new
            );

        private static final Map<String, BiFunction<LocalIdMap, LongMultiSet, ClassificationMetric>> ALL_CLASS_METRIC_FACTORIES = Map.of(
            F1Weighted.NAME, F1Weighted::new,
            F1Macro.NAME, (classIdMap, ignore) -> new F1Macro(classIdMap),
            GlobalAccuracy.NAME, (ignored1, ignored2) -> new GlobalAccuracy()
        );

        @RegExp
        private static final String NUMBER_OR_STAR = "(-?[\\d]+|\\*)";
        @RegExp
        private static final String CLASS_NAME_PATTERN = "(.+)";
        private static final Pattern SINGLE_CLASS_METRIC_PATTERN = Pattern.compile(
            CLASS_NAME_PATTERN + "\\(\\s*CLASS\\s*=\\s*" + NUMBER_OR_STAR + "\\s*\\)");

        private Parser() {}

        public static Iterable<String> singleClassMetrics() {
            return SINGLE_CLASS_METRIC_FACTORIES.keySet();
        }

        public static Iterable<String> allClassMetrics() { return ALL_CLASS_METRIC_FACTORIES.keySet(); }

        public static List<ClassificationMetricSpecification> parse(List<?> userSpecifications) {
            if (userSpecifications.isEmpty()) {
                throw new IllegalArgumentException(formatWithLocale("No metrics specified, we require at least one"));
            }

            if (userSpecifications.get(0) instanceof ClassificationMetricSpecification) {
                return (List<ClassificationMetricSpecification>) userSpecifications;
            }

            List<String> stringInput = (List<String>) userSpecifications;

            var mainMetric = stringInput.get(0).toUpperCase(Locale.ENGLISH);
            var errors = new ArrayList<String>();
            if (mainMetric.contains("*")) {
                errors.add(formatWithLocale(
                    "The primary (first) metric provided must be one of %s.",
                    String.join(", ", validPrimaryMetricExpressions())
                ));
            }
            List<String> badSpecifications = stringInput
                .stream()
                .filter(Parser::invalidSpecification)
                .collect(Collectors.toList());
            if (!badSpecifications.isEmpty()) {
                errors.add(errorMessage(badSpecifications));
            }
            if (!errors.isEmpty()) {
                throw new IllegalArgumentException(String.join(" ", errors));
            }
            return userSpecifications.stream()
                .map(Parser::parse)
                .distinct()
                .collect(Collectors.toList());
        }

        public static ClassificationMetricSpecification parse(Object userSpecification) {
            if (userSpecification instanceof ClassificationMetricSpecification) {
                return (ClassificationMetricSpecification) userSpecification;
            }

            if (userSpecification instanceof String) {
                String input = (String) userSpecification;

                var upperCaseSpecification = toUpperCaseWithLocale(input);
                if (upperCaseSpecification.equals(OUT_OF_BAG_ERROR.name())) {
                    return createSpecification((ignored, ignored2) -> Stream.of(OUT_OF_BAG_ERROR), upperCaseSpecification);
                }

                var matcher = SINGLE_CLASS_METRIC_PATTERN.matcher(upperCaseSpecification);
                if (!matcher.matches()) {
                    var allClassMetricGenerator = ALL_CLASS_METRIC_FACTORIES.get(upperCaseSpecification);
                    if (allClassMetricGenerator == null) {
                        throw new IllegalArgumentException(errorMessage(List.of(input)));
                    }
                    return createSpecification(
                        (classIdMap, classCounts) -> Stream.of(allClassMetricGenerator.apply(classIdMap, classCounts)),
                        upperCaseSpecification
                    );
                }

                var metricType = matcher.group(1);
                var classId = matcher.group(2);
                var metricGenerator = SINGLE_CLASS_METRIC_FACTORIES.get(metricType);

                if (metricGenerator == null) {
                    throw new IllegalArgumentException(errorMessage(List.of(input)));
                }

                Function<LocalIdMap, Stream<Metric>> metricsFactory = classId.equals("*")
                    ? classIdMap -> classIdMap.getMappings().map(idMap -> metricGenerator.value(idMap.key,idMap.value))
                    : classIdMap -> Stream.of(metricGenerator.value(Long.parseLong(classId), classIdMap.toMapped(Long.parseLong(classId))));

                return createSpecification(
                    (classIdMap, ignored) -> metricsFactory.apply(classIdMap),
                    formatWithLocale("%s(class=%s)", metricType, classId)
                );
            }

            throw new IllegalArgumentException(formatWithLocale(
                "Expected MetricSpecification or String. Got %s.",
                userSpecification.getClass().getSimpleName()
            ));
        }
        private static List<String> allValidMetricExpressions() {
            return validMetricExpressions(true);
        }

        private static List<String> validPrimaryMetricExpressions() {
            return validMetricExpressions(false);
        }

        private static List<String> validMetricExpressions(boolean includeSyntacticSugarMetrics) {
            var validExpressions = new LinkedList<>(MODEL_SPECIFIC_METRICS);

            var allClassExpressions = ALL_CLASS_METRIC_FACTORIES.keySet();
            validExpressions.addAll(allClassExpressions);

            for (String singleClassMetric : singleClassMetrics()) {
                if (includeSyntacticSugarMetrics) {
                    validExpressions.add(singleClassMetric + "(class=*)");
                }
                validExpressions.add(singleClassMetric + "(class=<class value>)");
            }
            return validExpressions;
        }

        private static String errorMessage(List<String> specifications) {
            return formatWithLocale(
                "Invalid metric expression%s %s. Available metrics are %s (case insensitive and space allowed between brackets).",
                specifications.size() == 1 ? "" : "s",
                specifications.stream().map(s -> "`" + s + "`").collect(Collectors.joining(", ")),
                String.join(", ", allValidMetricExpressions())
            );
        }

        private static boolean invalidSpecification(String userSpecification) {
            var upperCaseSpecification = userSpecification.toUpperCase(Locale.ENGLISH);
            if (MODEL_SPECIFIC_METRICS.contains(upperCaseSpecification)) return false;
            var matcher = SINGLE_CLASS_METRIC_PATTERN.matcher(upperCaseSpecification);
            if (matcher.matches()) return false;
            return !ALL_CLASS_METRIC_FACTORIES.containsKey(upperCaseSpecification);
        }

    }
}
