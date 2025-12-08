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
import org.neo4j.gds.GdlGraphStoreBuilder;
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

        telemetryLogger.logAlgorithm(42, "pageRank", testConfig, 1500L);

        var entry = extractLog(testLog, "Algorithm Telemetry:", TelemetryLoggerImpl.AlgorithmLogEntry.class);

        assertThat(entry).isEqualTo(new TelemetryLoggerImpl.AlgorithmLogEntry(
            42,
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

        telemetryLogger.logAlgorithm(1337, "louvain", configWithParams, 2500L);

        var entry = extractLog(testLog, "Algorithm Telemetry:", TelemetryLoggerImpl.AlgorithmLogEntry.class);

        assertThat(entry).isEqualTo(new TelemetryLoggerImpl.AlgorithmLogEntry(
            1337,
            "louvain",
            2500L,
            List.of("optionalParam")
        ));
    }

    @Test
    void shouldLogGraphTelemetry() throws JsonProcessingException {
        var testLog = new GdsTestLog();
        var telemetryLogger = new TelemetryLoggerImpl(testLog);

        var graphStore = new GdlGraphStoreBuilder()
            .gdl("(a:A), (b:A), (c:B), (a)-[:REL]->(b)-[:REL]->(c)")
            .name("test")
            .build();

        telemetryLogger.logGraph(graphStore);

        var entry = extractLog(testLog, "Graph Telemetry: ", TelemetryLoggerImpl.GraphLogEntry.class);

        assertThat(entry).isEqualTo(new TelemetryLoggerImpl.GraphLogEntry(
            System.identityHashCode(graphStore),
            3,
            2,
            2L,
            1L,
            false,
            false,
            false
        ));
    }

    <T> T extractLog(GdsTestLog log, String prefix, Class<T> type) throws JsonProcessingException {
        var messages = log.getMessages(GdsTestLog.INFO);
        assertThat(messages).hasSize(1);

        var message = messages.getFirst();

        return new ObjectMapper().readValue(
            message.replace(prefix, ""),
            type
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
