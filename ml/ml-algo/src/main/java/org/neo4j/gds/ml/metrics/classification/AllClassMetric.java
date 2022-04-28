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

import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.openjdk.jol.util.Multiset;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public enum AllClassMetric implements ClassificationCrossValidationMetric {
    F1_WEIGHTED {
        @Override
        public Comparator<Double> comparator() {
            return Comparator.naturalOrder();
        }

        @Override
        public double compute(
            HugeLongArray targets,
            HugeLongArray predictions,
            Multiset<Long> globalClassCounts
        ) {
            if (globalClassCounts.size() == 0) {
                return 0.0;
            }

            var weightedScores = globalClassCounts.keys().stream()
                .mapToDouble(target -> {
                    var weight = globalClassCounts.count(target);
                    return weight * new F1Score(target).compute(targets, predictions);
                });
            return weightedScores.sum() / globalClassCounts.size();
        }
    },
    F1_MACRO {
        @Override
        public Comparator<Double> comparator() {
            return Comparator.naturalOrder();
        }

        @Override
        public double compute(
            HugeLongArray targets,
            HugeLongArray predictions,
            Multiset<Long> globalClassCounts
        ) {
            var metrics = globalClassCounts.keys().stream()
                .map(F1Score::new)
                .collect(Collectors.toList());

            return metrics
                .stream()
                .mapToDouble(metric -> metric.compute(targets, predictions))
                .average()
                .orElse(-1);
        }
    },
    ACCURACY {
        @Override
        public Comparator<Double> comparator() {
            return Comparator.naturalOrder();
        }

        @Override
        public double compute(
            HugeLongArray targets,
            HugeLongArray predictions,
            Multiset<Long> globalClassCounts
        ) {
            long accuratePredictions = 0;
            assert targets.size() == predictions.size() : formatWithLocale(
                "Metrics require equal length targets and predictions. Sizes are %d and %d respectively.",
                targets.size(),
                predictions.size());

            for (long row = 0; row < targets.size(); row++) {
                long targetClass = targets.get(row);
                long predictedClass = predictions.get(row);
                if (predictedClass == targetClass) {
                    accuratePredictions++;
                }
            }

            if (targets.size() == 0) {
                return 0.0;
            }
            return BigDecimal.valueOf(accuratePredictions)
                .divide(BigDecimal.valueOf(targets.size()), 8, RoundingMode.UP)
                .doubleValue();
        }
    }
}
