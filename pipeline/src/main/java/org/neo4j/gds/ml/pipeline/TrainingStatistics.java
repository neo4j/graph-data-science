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
import org.neo4j.gds.ml.metrics.StatsMap;
import org.neo4j.gds.ml.models.TrainerConfig;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class TrainingStatistics {

    private final StatsMap trainStats;
    private final StatsMap validationStats;
    private final List<Metric> metrics;
    private final Map<Metric, Double> testScores;
    private final Map<Metric, Double> outerTrainScores;

    public TrainingStatistics(List<Metric> metrics) {
        this.trainStats = StatsMap.create(metrics);
        this.validationStats = StatsMap.create(metrics);
        this.metrics = metrics;
        this.testScores = new HashMap<>();
        this.outerTrainScores = new HashMap<>();
    }

    public TrainerConfig bestParameters() {
        var modelStats = validationStats.getMetricStats(evaluationMetric());
        return Collections.max(modelStats, ModelStats.COMPARE_AVERAGE).params();
    }

    @TestOnly
    public List<ModelStats> getTrainStats(Metric metric) {
        return trainStats.getMetricStats(metric);
    }

    @TestOnly
    public List<ModelStats> getValidationStats(Metric metric) {
        return validationStats.getMetricStats(metric);
    }

    /**
     * Turns this class into a Cypher map, to be returned in a procedure YIELD field.
     * This is intentionally omitting the test scores.
     * These can be added to extend the return surface later.
     */
    public Map<String, Object> toMap() {
        return Map.of(
            "bestParameters", bestParameters().toMap(),
            "trainStats", trainStats.toMap(),
            "validationStats", validationStats.toMap()
        );
    }

    public Map<Metric, BestMetricData> metricsForWinningModel() {
        TrainerConfig bestParameters = bestParameters();

        return metrics.stream().collect(Collectors.toMap(
            Function.identity(),
            metric -> BestMetricData.of(
                findBestModelStats(trainStats.getMetricStats(metric), bestParameters),
                findBestModelStats(validationStats.getMetricStats(metric), bestParameters),
                outerTrainScores.get(metric),
                testScores.get(metric)
            )
        ));
    }

    private static BestModelStats findBestModelStats(
        List<ModelStats> metricStatsForModels,
        TrainerConfig bestParameters
    ) {
        return metricStatsForModels.stream()
            .filter(metricStatsForModel -> metricStatsForModel.params() == bestParameters)
            .findFirst()
            .map(BestModelStats::of)
            .orElseThrow();
    }

    public double getMainValidationMetric(int trial) {
        return findModelValidationAvg(trial).get(evaluationMetric());
    }

    public Map<Metric, Double> findModelValidationAvg(int trial) {
        return findModelAvg(trial, validationStats);
    }

    public Map<Metric, Double> findModelTrainAvg(int trial) {
        return findModelAvg(trial, trainStats);
    }

    private Map<Metric, Double> findModelAvg(int trial, StatsMap statsMap) {
        return metrics.stream()
            .collect(Collectors.toMap(
                metric -> metric,
                metric -> statsMap.getMetricStats(metric).get(trial).avg()
            ));
    }

    public Metric evaluationMetric() {
        return metrics.get(0);
    }

    public void addValidationStats(Metric metric, ModelStats stats) {
        validationStats.add(metric, stats);
    }

    public void addTrainStats(Metric metric, ModelStats stats) {
        trainStats.add(metric, stats);
    }

    public void addTestScore(Metric metric, double score) {
        testScores.put(metric, score);
    }

    public void addOuterTrainScore(Metric metric, double score) {
        outerTrainScores.put(metric, score);
    }

    public Map<Metric, Double> winningModelTestMetrics() {
        return testScores;
    }

    public Map<Metric, Double> winningModelOuterTrainMetrics() {
        return outerTrainScores;
    }

    public double getBestTrialScore() {
        return validationStats.getMetricStats(evaluationMetric())
            .stream()
            .mapToDouble(ModelStats::avg)
            .max()
            .getAsDouble();
    }

    public int getBestTrialIdx() {
        return validationStats.getMetricStats(evaluationMetric())
            .stream()
            .map(ModelStats::avg)
            .collect(Collectors.toList())
            .indexOf(getBestTrialScore());
    }
}
