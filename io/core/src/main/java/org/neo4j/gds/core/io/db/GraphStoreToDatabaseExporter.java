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
package org.neo4j.gds.core.io.db;

import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.io.GraphStoreExporter;
import org.neo4j.gds.core.io.GraphStoreInput;
import org.neo4j.gds.core.io.NeoNodeProperties;
import org.neo4j.gds.core.utils.ClockService;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Log;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class GraphStoreToDatabaseExporter extends GraphStoreExporter<GraphStoreToDatabaseExporterConfig> {

    private final GdsParallelBatchImporter parallelBatchImporter;

    public static GraphStoreToDatabaseExporter of(
        GraphStore graphStore,
        GraphDatabaseAPI api,
        GraphStoreToDatabaseExporterConfig config,
        Log log,
        ProgressTracker progressTracker
    ) {
        return of(graphStore, api, config, Optional.empty(), log, progressTracker);
    }

    public static GraphStoreToDatabaseExporter of(
        GraphStore graphStore,
        GraphDatabaseAPI api,
        GraphStoreToDatabaseExporterConfig config,
        Optional<NeoNodeProperties> neoNodeProperties,
        Log log,
        ProgressTracker progressTracker
    ) {
        return new GraphStoreToDatabaseExporter(graphStore, api, config, neoNodeProperties, log, progressTracker);
    }

    private GraphStoreToDatabaseExporter(
        GraphStore graphStore,
        GraphDatabaseAPI api,
        GraphStoreToDatabaseExporterConfig config,
        Optional<NeoNodeProperties> neoNodeProperties,
        Log log,
        ProgressTracker progressTracker
    ) {
        super(graphStore, config, neoNodeProperties);
        var executionMonitor = ProgressTrackerExecutionMonitor.of(
            progressTracker,
            ClockService.clock(),
            config.executionMonitorCheckMillis(),
            TimeUnit.MILLISECONDS
        );
        this.parallelBatchImporter = GdsParallelBatchImporter.fromDb(api, config, log, executionMonitor);
    }

    @Override
    public void export(GraphStoreInput graphStoreInput) {
        parallelBatchImporter.writeDatabase(graphStoreInput, false);
    }

    @Override
    protected IdMappingType idMappingType() {
        return IdMappingType.MAPPED;
    }
}
