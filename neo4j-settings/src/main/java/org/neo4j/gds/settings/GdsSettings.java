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
package org.neo4j.gds.settings;

import org.neo4j.graphdb.config.Setting;

import java.nio.file.Path;
import java.time.Duration;

/**
 * The facade to GDS settings in Neo4j.
 */
public final class GdsSettings {
    private GdsSettings() {}

    public static Setting<Path> exportLocation() {
        return GraphStoreExportSettings.export_location_setting;
    }

    public static Setting<Boolean> metricsServerEnabled() {
        return GdsMetricsSettings.gds_metrics_server_enabled;
    }

    public static Setting<Boolean> progressTrackingEnabled() {
        return ProgressFeatureSettings.progress_tracking_enabled;
    }

    public static Setting<Duration> taskRetentionPeriod() {
        return ProgressFeatureSettings.task_retention_period;
    }

    public static Setting<Boolean> telemetryLoggingEnabled() {
        return GdsTelemetrySettings.gds_telemetry_logging_enabled;
    }

    public static Setting<Boolean> validateUsingMaxMemoryEstimation() {
        return MemoryEstimationSettings.validate_using_max_memory_estimation;
    }
}
