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
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.ToMapConvertible;
import org.neo4j.gds.core.JobId;

public interface TelemetryLogger {

    /**
     * Called explicitly by every graph-creation code path. There is no central
     * catalog hook; new endpoints that add a graph to the catalog must call this.
     */
    void logGraph(GraphStore graphStore, long projectMillis);

    default void logAlgorithm(int graphIdentifier, String algorithm, AlgoBaseConfig config, long computeMillis, long startTime) {
        logAlgorithm(graphIdentifier, config.jobId(), algorithm, computeMillis, config, startTime);
    };

    void logAlgorithm(int graphId, JobId jobId, String algorithm, long computeMillis, ToMapConvertible config, long startTime);

    TelemetryLogger DISABLED = new TelemetryLogger() {
        @Override
        public void logGraph(
            GraphStore graphStore,
            long projectMillis
        ) {

        }

        @Override
        public void logAlgorithm(int graphId, JobId jobId, String algorithm, long computeMillis, ToMapConvertible config, long startTime) {

        }
    };
}
