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
package org.neo4j.gds.metrics.telemetry;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.logging.Log;

import java.util.List;

public class TelemetryLoggerImpl implements TelemetryLogger {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Log log;

    public TelemetryLoggerImpl(Log log) {
        this.log = log;
    }

    @Override
    public void log_algorithm(String algorithm, AlgoBaseConfig config, long computeMillis) {
        try {
            var configuredParameters = ConfigAnalyzer.nonDefaultParameters(config, log);

            var logEntry = new AlgorithmLogEntry(algorithm, computeMillis, configuredParameters);

            var jsonEntry = OBJECT_MAPPER.writeValueAsString(logEntry);
            log.info("Algorithm Telemetry: %s", jsonEntry);
        } catch (Exception e) {
            log.warn("Failed to log telemetry: %s", e.getMessage());
        }
    }

    public static record AlgorithmLogEntry(
        String algorithm,
        long computeMillis,
        List<String> configuredParameters
    ) {

    }
}
