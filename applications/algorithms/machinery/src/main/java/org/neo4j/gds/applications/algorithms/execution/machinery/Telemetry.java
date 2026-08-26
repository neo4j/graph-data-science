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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimingsBuilder;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.metrics.telemetry.TelemetryLogger;

import java.util.function.Supplier;

/**
 * Log telemetry when running algorithm.
 * I cannot explain how this is different from metrics.
 */
class Telemetry {
    private final Supplier<ComputationTimer> computationTimerSupplier;
    private final TelemetryLogger telemetryLogger;
    private final Timer timer;

    Telemetry(
        Supplier<ComputationTimer> computationTimerSupplier,
        TelemetryLogger telemetryLogger,
        Timer timer
    ) {
        this.computationTimerSupplier = computationTimerSupplier;
        this.telemetryLogger = telemetryLogger;
        this.timer = timer;
    }

    static Telemetry create(TelemetryLogger telemetryLogger) {
        Supplier<ComputationTimer> computationTimerSupplier = ComputationTimer::create;
        var timer = new Timer();

        return new Telemetry(computationTimerSupplier, telemetryLogger, timer);
    }

    <CONFIGURATION extends AlgoBaseConfig, RESULT> RESULT runAlgorithmAndLogTelemetry(
        AlgorithmProcessingTimingsBuilder timingsBuilder,
        GraphId graphId,
        Label label,
        CONFIGURATION configuration,
        ConstructAndRun<RESULT> constructAndRun,
        Graph graph
    ) {
        var computationTimer = computationTimerSupplier.get();

        var result = timer.runAlgorithmAndRecordTiming(computationTimer, constructAndRun, graph);

        timingsBuilder.withComputeMillis(computationTimer.durationInMilliseconds());

        telemetryLogger.logAlgorithm(
            graphId.value(),
            label.asString(),
            configuration,
            computationTimer.durationInMilliseconds(),
            computationTimer.startTimeInEpochMilliseconds()
        );

        return result;
    }
}
