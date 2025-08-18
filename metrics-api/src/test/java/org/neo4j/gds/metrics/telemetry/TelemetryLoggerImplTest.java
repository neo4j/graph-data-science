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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.logging.GdsTestLog;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryLoggerImplTest {

    @Test
    void shouldLogSuccessfulAlgorithmTelemetry() throws JsonProcessingException {
        var testLog = new GdsTestLog();
        var telemetryLogger = new TelemetryLoggerImpl(testLog);
        var testConfig = new TelemetryConfigImpl(CypherMapWrapper.empty());

        telemetryLogger.log_algorithm("pageRank", testConfig, 1500L);

        TelemetryLoggerImpl.AlgorithmLogEntry entry = extractLog(testLog);

        assertThat(entry).isEqualTo(new TelemetryLoggerImpl.AlgorithmLogEntry(
            "pageRank",
            1500L,
            List.of()
        ));
    }

    @Test
    void shouldLogAlgorithmTelemetryWithConfiguredParameters() throws JsonProcessingException {
        var testLog = new GdsTestLog();
        var telemetryLogger = new TelemetryLoggerImpl(testLog);

        var configWithParams = new TelemetryConfigImpl(
            CypherMapWrapper.create(Map.of(
                "optionalParam", "custom-value"
            ))
        );

        telemetryLogger.log_algorithm("louvain", configWithParams, 2500L);

        TelemetryLoggerImpl.AlgorithmLogEntry entry = extractLog(testLog);

        assertThat(entry).isEqualTo(new TelemetryLoggerImpl.AlgorithmLogEntry(
            "louvain",
            2500L,
            List.of("optionalParam")
        ));
    }

    TelemetryLoggerImpl.AlgorithmLogEntry extractLog(GdsTestLog log) throws JsonProcessingException {
        var messages = log.getMessages(GdsTestLog.INFO);
        assertThat(messages).hasSize(1);

        var message = messages.getFirst();

        return new ObjectMapper().readValue(
            message.replace("Algorithm Telemetry:", ""),
            TelemetryLoggerImpl.AlgorithmLogEntry.class
        );
    }

    // Helper interface for testing - matches pattern from ConfigAnalyzerTest
    interface BaseTestConfig extends AlgoBaseConfig {
        @Configuration.Key("stringParam")
        default String stringParam() {
            return "default-value";
        }

        @Configuration.Key("optionalParam")
        Optional<String> optionalParam();
    }

    @Configuration
    interface TelemetryConfig extends BaseTestConfig {
    }
}
