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

import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;

import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public interface Metric {
    Map<String, Function<Long, Metric>> SINGLE_CLASS_METRICS = Map.of(
        "F1", F1Score::new
    );
    Pattern SINGLE_CLASS_METRIC_PATTERN = Pattern.compile("([A-Z0-9_]+)\\(CLASS=([0-9]+)\\)");

    double compute(HugeLongArray targets, HugeLongArray predictions, HugeLongArray globalTargets);

    String asString();

    static List<Metric> resolveMetrics(List<String> metrics) {
        return metrics.stream()
            .map(Metric::resolveMetric)
            .collect(Collectors.toList());
    }

    static Metric resolveMetric(String metric) {
        var normalizedMetric = metric.replace("[\\s]+", "")
            .toUpperCase(Locale.ENGLISH);
        var matcher = SINGLE_CLASS_METRIC_PATTERN.matcher(normalizedMetric);
        if (matcher.matches()) {
            var metricType = matcher.group(1);
            var classId = Long.parseLong(matcher.group(2));
            if (SINGLE_CLASS_METRICS.containsKey(metricType)) {
                return SINGLE_CLASS_METRICS.get(metricType).apply(classId);
            }
        }
        try {
            return AllClassMetric.valueOf(normalizedMetric);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(formatWithLocale(
                "Invalid metric expression `%s`. Available metrics are %s",
                metric,
                String.join(", ", validMetricExpressions())
            ));
        }
    }

    static List<String> validMetricExpressions() {
        var allClassExpressions = AllClassMetric.values();
        var validExpressions = new LinkedList<String>();
        for (AllClassMetric allClassExpression : allClassExpressions) {
            validExpressions.add(allClassExpression.name());
        }
        for (String singleClassMetric : SINGLE_CLASS_METRICS.keySet()) {
            validExpressions.add(singleClassMetric + "(class=<class value>)");
        }
        return validExpressions;
    }

    static List<String> metricsToString(List<Metric> metrics) {
        return metrics.stream()
            .map(Metric::asString)
            .collect(Collectors.toList());
    }
}
