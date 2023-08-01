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
package org.neo4j.gds.ml.metrics.regression;

import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.ml.metrics.Metric;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.gds.utils.StringFormatting.toUpperCaseWithLocale;

public enum RegressionMetrics implements Metric {
    MEAN_SQUARED_ERROR {
        @Override
        public Comparator<Double> comparator() {
            return Comparator.reverseOrder();
        }

        @Override
        public double compute(HugeDoubleArray targets, HugeDoubleArray predictions) {
            long numberOfExamples = targets.size();
            assert numberOfExamples == predictions.size();

            double squaredError = 0;
            for (long i = 0; i < numberOfExamples; i++) {
                var error = predictions.get(i) - targets.get(i);
                squaredError += error * error;
            }

            return squaredError / numberOfExamples;
        }
    },
    ROOT_MEAN_SQUARED_ERROR {
        @Override
        public Comparator<Double> comparator() {
            return Comparator.reverseOrder();
        }

        @Override
        public double compute(HugeDoubleArray targets, HugeDoubleArray predictions) {
            return Math.sqrt(MEAN_SQUARED_ERROR.compute(targets, predictions));
        }
    },
    MEAN_ABSOLUTE_ERROR {
        @Override
        public Comparator<Double> comparator() {
            return Comparator.reverseOrder();
        }

        @Override
        public double compute(HugeDoubleArray targets, HugeDoubleArray predictions) {
            long numberOfExamples = targets.size();
            assert numberOfExamples == predictions.size();

            double totalError = 0;
            for (long i = 0; i < numberOfExamples; i++) {
                totalError += Math.abs(targets.get(i) - predictions.get(i));
            }

            return totalError / numberOfExamples;
        }
    };

    public abstract double compute(HugeDoubleArray targets, HugeDoubleArray predictions);

    private static final List<String> VALUES = Arrays
        .stream(RegressionMetrics.values())
        .map(RegressionMetrics::name)
        .collect(Collectors.toList());

    public static List<RegressionMetrics> parseList(List<?> input) {
        return input.stream().map(RegressionMetrics::parse).collect(Collectors.toList());
    }

    public static RegressionMetrics parse(Object input) {
        if (input instanceof String) {
            var inputString = toUpperCaseWithLocale((String) input);

            if (VALUES.contains(inputString)) {
                return RegressionMetrics.valueOf(inputString.toUpperCase(Locale.ENGLISH));
            }

            throw new IllegalArgumentException(formatWithLocale(
                "RegressionMetric `%s` is not supported. Must be one of: %s.",
                inputString,
                VALUES
            ));
        } else if (input instanceof RegressionMetrics) {
            return (RegressionMetrics) input;
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected RegressionMetric or String. Got %s.",
            input.getClass().getSimpleName()
        ));
    }

    public static List<String> toString(List<RegressionMetrics> metrics) {
        return metrics.stream().map(RegressionMetrics::name).collect(Collectors.toList());
    }
}
