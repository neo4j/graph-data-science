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
package org.neo4j.gds.ml.pipeline;

import org.jetbrains.annotations.TestOnly;
import org.neo4j.gds.ml.metrics.BestMetricData;
import org.neo4j.gds.ml.metrics.BestModelStats;
import org.neo4j.gds.ml.metrics.Metric;
import org.neo4j.gds.ml.metrics.ModelStats;
import org.neo4j.gds.ml.models.TrainerConfig;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TrainingStatistics {

    private final TrainerConfig bestParameters;

    private final Map<? extends Metric, List<ModelStats>> trainStats;

    private final Map<? extends Metric, List<ModelStats>> validationStats;

    public TrainingStatistics(
        TrainerConfig bestParameters,
        Map<? extends Metric, List<ModelStats>> trainStats,
        Map<? extends Metric, List<ModelStats>> validationStats
    ) {
        this.bestParameters = bestParameters;
        this.trainStats = trainStats;
        this.validationStats = validationStats;
    }

    public TrainerConfig bestParameters() {
        return bestParameters;
    }

    @TestOnly
    public List<ModelStats> getTrainStats(Metric metric) {
        return trainStats.get(metric);
    }

    @TestOnly
    public List<ModelStats> getValidationStats(Metric metric) {
        return validationStats.get(metric);
    }

    public Map<String, Object> toMap() {
        return Map.of(
            "bestParameters", bestParameters().toMap(),
            "trainStats", convertToCypher(trainStats),
            "validationStats", convertToCypher(validationStats)
        );
    }

    public Map<Metric, BestMetricData> finalizeMetrics(
        Map<? extends Metric, Double> outerTrainMetrics,
        Map<? extends Metric, Double> testMetrics
    ) {
        var metrics = validationStats.keySet();

        return metrics.stream().collect(Collectors.toMap(
            Function.identity(),
            metric -> BestMetricData.of(
                findBestModelStats(trainStats.get(metric)),
                findBestModelStats(validationStats.get(metric)),
                outerTrainMetrics.get(metric),
                testMetrics.get(metric)
            )
        ));
    }

    private BestModelStats findBestModelStats(List<ModelStats> metricStatsForModels) {
        return metricStatsForModels.stream()
            .filter(metricStatsForModel -> metricStatsForModel.params() == bestParameters)
            .findFirst()
            .map(BestModelStats::of)
            .orElseThrow();
    }

    private static Map<String, List<Map<String, Object>>> convertToCypher(Map<? extends Metric, List<ModelStats>> metricStats) {
        return metricStats.entrySet().stream().collect(Collectors.toMap(
            entry -> entry.getKey().name(),
            value -> value.getValue().stream().map(ModelStats::toMap).collect(Collectors.toList())
        ));
    }
}
