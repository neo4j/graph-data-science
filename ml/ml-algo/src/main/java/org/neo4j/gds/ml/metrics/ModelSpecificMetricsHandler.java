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

import org.jetbrains.annotations.TestOnly;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public final class ModelSpecificMetricsHandler {
    public static final ModelSpecificMetricsHandler NOOP = new ModelSpecificMetricsHandler(List.of(), (metric, score) -> {});
    private final List<? extends Metric> metrics;

    private final BiConsumer<Metric, Double> metricConsumer;

    private ModelSpecificMetricsHandler(List<? extends Metric> metrics, BiConsumer<Metric, Double> metricConsumer) {
        this.metrics = metrics;
        this.metricConsumer = metricConsumer;
    }

    public static ModelSpecificMetricsHandler of(List<? extends Metric> metrics, BiConsumer<Metric, Double> metricConsumer) {
        return new ModelSpecificMetricsHandler(
            metrics.stream().filter(Metric::isModelSpecific).collect(Collectors.toList()),
            metricConsumer
        );
    }

    public static ModelSpecificMetricsHandler of(List<? extends Metric> metrics, ModelStatsBuilder modelStatsBuilder) {
        return ModelSpecificMetricsHandler.of(metrics, modelStatsBuilder::update);
    }

    public boolean isRequested(Metric metric) {
        return metrics.contains(metric);
    }

    public void handle(Metric metric, double score) {
        if (!isRequested(metric)) {
            throw new IllegalStateException("Should not handle a metric which is not requested");
        }
        metricConsumer.accept(metric, score);
    }

    @TestOnly
    public static ModelSpecificMetricsHandler ignoringResult(List<Metric> metrics) {
        return new ModelSpecificMetricsHandler(metrics, (a,b) -> {});
    }
}
