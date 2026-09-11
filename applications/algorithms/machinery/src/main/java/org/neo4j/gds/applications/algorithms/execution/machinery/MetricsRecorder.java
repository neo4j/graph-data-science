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
package org.neo4j.gds.applications.algorithms.execution.machinery;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimingsBuilder;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.metrics.algorithms.AlgorithmMetricsService;

/**
 * Capture metrics of successfully-run algorithms
 */
class MetricsRecorder {
    private final Log log;
    private final AlgorithmMetricsService algorithmMetricsService;
    private final Telemetry telemetry;

    MetricsRecorder(Log log, AlgorithmMetricsService algorithmMetricsService, Telemetry telemetry) {
        this.log = log;
        this.algorithmMetricsService = algorithmMetricsService;
        this.telemetry = telemetry;
    }

    <CONFIGURATION extends AlgoBaseConfig, RESULT> RESULT recordMetricsAndRunAlgorithm(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        GraphId graphId,
        Label label,
        CONFIGURATION configuration,
        ConstructAndRun<RESULT> constructAndRun,
        Graph graph,
        GraphStore graphStore
    ) {
        try (var executionMetric = algorithmMetricsService.create(label.asString())) {
            executionMetric.start();

            try {
                return telemetry.runAlgorithmAndLogTelemetry(
                    timingsBuilder,
                    graphId,
                    label,
                    configuration,
                    constructAndRun,
                    graph,
                    graphStore
                );
            } catch (RuntimeException e) {
                log.warn("computation failed, halting metrics gathering", e);
                executionMetric.failed(e);
                throw e;
            }
        }
    }
}
