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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.annotation.Configuration;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.logging.GdsTestLog;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryLoggerTest {

    private GdsTestLog testLog;
    private TelemetryLogger telemetryLogger;
    private TelemetryConfigImpl testConfig;

    @BeforeEach
    void setUp() {
        testLog = new GdsTestLog();
        telemetryLogger = new TelemetryLogger(testLog);
        testConfig = new TelemetryConfigImpl(CypherMapWrapper.empty());
    }

    @Test
    void shouldLogSuccessfulAlgorithmTelemetry() {
        telemetryLogger.log_algorithm("pageRank", testConfig, 1500L);

        testLog.assertContainsMessage(GdsTestLog.INFO, "Algorithm Telemetry:");
        testLog.assertContainsMessage(GdsTestLog.INFO, "pageRank");
        testLog.assertContainsMessage(GdsTestLog.INFO, "1500");
        testLog.assertContainsMessage(GdsTestLog.INFO, "configuredParameters");
        testLog.assertContainsMessage(GdsTestLog.INFO, "[]");
    }

    @Test
    void shouldLogAlgorithmTelemetryWithConfiguredParameters() {
        var configWithParams = new TelemetryConfigImpl(
            CypherMapWrapper.create(Map.of(
                "optionalParam", "custom-value"
            ))
        );

        telemetryLogger.log_algorithm("louvain", configWithParams, 2500L);

        testLog.assertContainsMessage(GdsTestLog.INFO, "Algorithm Telemetry:");
        testLog.assertContainsMessage(GdsTestLog.INFO, "louvain");
        testLog.assertContainsMessage(GdsTestLog.INFO, "2500");
        testLog.assertContainsMessage(GdsTestLog.INFO, "optionalParam");
    }

    @Test
    void shouldIncludeAllRequiredFieldsInLogEntry() {
        telemetryLogger.log_algorithm("wcc", testConfig, 750L);

        var infoMessages = testLog.getMessages(GdsTestLog.INFO);
        assertThat(infoMessages).hasSize(1);

        String logMessage = infoMessages.get(0);
        assertThat(logMessage).contains("Algorithm Telemetry:");
        assertThat(logMessage).contains("\"algorithm\":\"wcc\"");
        assertThat(logMessage).contains("\"computeMillis\":750");
        assertThat(logMessage).contains("\"configuredParameters\":");
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