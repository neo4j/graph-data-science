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
package org.neo4j.gds.ml.metrics;

import org.immutables.value.Value;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.ml.models.TrainerConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ValueClass
public interface ModelCandidateStats extends ToMapConvertible {
    TrainerConfig trainerConfig();
    Map<Metric, EvaluationScores> trainingStats();
    Map<Metric, EvaluationScores> validationStats();

    @Override
    @Value.Auxiliary
    @Value.Derived
    default Map<String, Object> toMap() {
        return Map.of(
            "parameters", trainerConfig().toMapWithTrainerMethod(),
            "metrics", renderMetrics()
        );
    }

    @Value.Derived
    default Map<String, Map<String, Object>> renderMetrics(
        Map<Metric, Double> testMetrics,
        Map<Metric, Double> outerTrainMetrics
    ) {
        return renderMetrics(Optional.of(testMetrics), Optional.of(outerTrainMetrics));
    }

    private Map<String, Map<String, Object>> renderMetrics() {
        return renderMetrics(Optional.empty(), Optional.empty());
    }

    private Map<String, Map<String, Object>> renderMetrics(
        Optional<Map<Metric, Double>> testMetrics,
        Optional<Map<Metric, Double>> outerTrainMetrics
    ) {
        return metrics().stream().collect(Collectors.toMap(
            Object::toString,
            metric -> {
                var result = new HashMap<String, Object>();
                if (trainingStats().containsKey(metric)) {
                    result.put("train", trainingStats().get(metric).toMap());
                }
                if (validationStats().containsKey(metric)) {
                    result.put("validation", validationStats().get(metric).toMap());
                }
                testMetrics.ifPresent(test -> {
                    if (test.containsKey(metric)) {
                        result.put("test", test.get(metric));
                    }
                });
                outerTrainMetrics.ifPresent(outerTrain -> {
                    if (outerTrain.containsKey(metric)) {
                        result.put("outerTrain", outerTrain.get(metric));
                    }
                });
                return result;
            }
        ));
    }

    private List<Metric> metrics() {
        // we assume that all metrics in test and outerTrain are also in train
        return Stream
            .concat(trainingStats().keySet().stream(), validationStats().keySet().stream())
            .distinct()
            .collect(Collectors.toList());
    }

    static ModelCandidateStats of(
        TrainerConfig trainerConfig,
        Map<Metric, EvaluationScores> trainStats,
        Map<Metric, EvaluationScores> validationStats
    ) {
        return ImmutableModelCandidateStats.of(trainerConfig, trainStats, validationStats);
    }
}
