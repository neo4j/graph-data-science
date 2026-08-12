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

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.logging.Log;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

public class TelemetryLoggerImpl implements TelemetryLogger {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final Log log;

    public TelemetryLoggerImpl(Log log) {
        this.log = log;
    }

    @Override
    public void logGraph(
        GraphStore graphStore,
        long projectMillis
    ) {
        try {
            var logEntry = new GraphLogEntry(
                System.identityHashCode(graphStore),
                graphStore.nodeCount(),
                graphStore.relationshipCount(),
                graphStore.nodeLabels().size(),
                graphStore.relationshipTypes().size(),
                !graphStore.nodePropertyKeys().isEmpty(),
                graphStore.nodePropertyKeys().size(),
                !graphStore.relationshipPropertyKeys().isEmpty(),
                graphStore.relationshipPropertyKeys().size(),
                !graphStore.inverseIndexedRelationshipTypes().isEmpty(),
                projectMillis
            );
            var jsonEntry = OBJECT_MAPPER.writeValueAsString(logEntry);
            log.info("Graph Telemetry: %s", jsonEntry);
        } catch (Exception e) {
            log.warn("Failed to log telemetry: %s", e.getMessage());
        }
    }

    @Override
    public void logAlgorithm(int graphId, JobId jobId, String algorithm, long computeMillis, ToMapConvertible config, long startTime) {
        try {
            var configuredParameters = ConfigAnalyzer.nonDefaultParameters(config, log);

            var logEntry = new AlgorithmLogEntry(graphId, jobId.asString(), algorithm, computeMillis, configuredParameters, startTime);

            var jsonEntry = OBJECT_MAPPER.writeValueAsString(logEntry);
            log.info("Algorithm Telemetry: %s", jsonEntry);
        } catch (Exception e) {
            log.warn("Failed to log telemetry: %s", e.getMessage());
        }
    }

    public record GraphLogEntry(
        int graphId,
        long nodeCount,
        long relationshipCount,
        long labelCount,
        long typeCount,
        boolean hasNodeProperties,
        long nodePropertyCount,
        boolean hasRelationshipProperties,
        long relationshipPropertyCount,
        boolean hasInverseIndexedRelationships,
        long projectMillis
    ) {

    }

    public record AlgorithmLogEntry(
        int graphId,
        String jobId,
        String algorithm,
        long computeMillis,
        List<String> configuredParameters,
        // as jobId can be set by the user we use jobId + startTime. and we dont want to introduce yet another id
        long startTime
    ) {

    }
}
