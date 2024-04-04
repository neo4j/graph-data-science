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

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.gds.compat.CompatExecutionMonitor;
import org.neo4j.gds.compat.GdsDatabaseLayout;
import org.neo4j.gds.compat.GraphDatabaseApiProxy;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.core.utils.ProgressTimer;
import org.neo4j.gds.settings.Neo4jSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Collectors;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.gds.core.io.GraphStoreExporter.DIRECTORY_IS_WRITABLE;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class GdsParallelBatchImporter {

    private final GraphStoreToDatabaseExporterParameters parameters;
    private final Log log;
    private final CompatExecutionMonitor executionMonitor;

    private final FileSystemAbstraction fileSystem;
    private final LogService logService;
    private final Config databaseConfig;
    private final DatabaseManagementService dbms;
    private final GraphDatabaseService databaseService;

    static GdsParallelBatchImporter fromDb(
        GraphDatabaseService databaseService,
        GraphStoreToDatabaseExporterParameters parameters,
        Log log,
        CompatExecutionMonitor executionMonitor
    ) {
        var dbms = GraphDatabaseApiProxy.resolveDependency(databaseService, DatabaseManagementService.class);
        var fs = GraphDatabaseApiProxy.resolveDependency(databaseService, FileSystemAbstraction.class);
        var logService = GraphDatabaseApiProxy.resolveDependency(databaseService, LogService.class);
        var databaseConfig = GraphDatabaseApiProxy.resolveDependency(databaseService, Config.class);
        return new GdsParallelBatchImporter(
            parameters,
            log,
            executionMonitor,
            dbms,
            databaseService,
            fs,
            logService,
            databaseConfig
        );
    }

    public static GdsParallelBatchImporter fromDbms(
        DatabaseManagementService dbms,
        GraphStoreToDatabaseExporterParameters parameters,
        Log log,
        CompatExecutionMonitor executionMonitor
    ) {
        var databaseService = dbms.database(SYSTEM_DATABASE_NAME);
        var fs = GraphDatabaseApiProxy.resolveDependency(databaseService, FileSystemAbstraction.class);
        var logService = GraphDatabaseApiProxy.resolveDependency(databaseService, LogService.class);
        var databaseConfig = GraphDatabaseApiProxy.resolveDependency(databaseService, Config.class);
        return new GdsParallelBatchImporter(
            parameters,
            log,
            executionMonitor,
            dbms,
            databaseService,
            fs,
            logService,
            databaseConfig
        );
    }

    private GdsParallelBatchImporter(
        GraphStoreToDatabaseExporterParameters parameters,
        Log log,
        CompatExecutionMonitor executionMonitor,
        DatabaseManagementService dbms,
        GraphDatabaseService databaseService,
        FileSystemAbstraction fileSystem,
        LogService logService,
        Config databaseConfig
    ) {
        this.parameters = parameters;
        this.log = log;
        this.executionMonitor = executionMonitor;
        this.dbms = dbms;
        this.databaseService = databaseService;
        this.fileSystem = fileSystem;
        this.logService = logService;

        var configBuilder = Config
            .newBuilder()
            .fromConfig(databaseConfig)
            .set(Neo4jSettings.neo4jHome(), databaseConfig.get(Neo4jSettings.neo4jHome()))
            .set(GraphDatabaseSettings.data_directory, databaseConfig.get(GraphDatabaseSettings.data_directory));

        Neo4jProxy.configureRecordFormat(configBuilder, parameters.recordFormat());

        this.databaseConfig = configBuilder.build();
    }

    public void writeDatabase(Input input, boolean startDatabase) {
        log.info("Database import started");

        var importTimer = ProgressTimer.start();

        var databaseLayout = Neo4jProxy.databaseLayout(databaseConfig, parameters.dbName());

        validateWritableDirectories(databaseLayout);
        validateDatabaseDoesNotExist(databaseLayout);

        var lifeSupport = new LifeSupport();

        try {
            if (parameters.force()) {
                fileSystem.deleteRecursively(databaseLayout.databaseDirectory());
                fileSystem.deleteRecursively(databaseLayout.getTransactionLogsDirectory());
            }

            var logService = getLogService();
            var collector = getCollector();
            var jobScheduler = lifeSupport.add(JobSchedulerFactory.createScheduler());

            lifeSupport.start();

            var batchImporter = instantiateBatchImporter(
                databaseLayout,
                logService,
                collector,
                jobScheduler
            );
            batchImporter.doImport(input);
            log.info(formatWithLocale("Database import finished after %s ms", importTimer.stop().getDuration()));

            if (startDatabase) {
                var dbStartTimer = ProgressTimer.start();
                if (createAndStartDatabase()) {
                    log.info(
                        formatWithLocale(
                            "Database created and started after %s ms",
                            dbStartTimer.stop().getDuration()
                        )
                    );
                } else {
                    log.error("Unable to start database " + parameters.dbName());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            lifeSupport.shutdown();
        }
    }

    private void validateWritableDirectories(GdsDatabaseLayout databaseLayout) {
        DIRECTORY_IS_WRITABLE.validate(databaseLayout.databaseDirectory());
        DIRECTORY_IS_WRITABLE.validate(databaseLayout.getTransactionLogsDirectory());
    }

    private void validateDatabaseDoesNotExist(GdsDatabaseLayout databaseLayout) {
        var metaDataPath = databaseLayout.metadataStore();
        var dbExists = Files.exists(metaDataPath) && Files.isReadable(metaDataPath);
        if (dbExists && !parameters.force()) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "The database [%s] already exists. The graph export procedure can only create new databases.",
                    parameters.dbName()
                )
            );
        }
    }

    private LogService getLogService() {
        return parameters.enableDebugLog()
            ? logService
            : NullLogService.getInstance();
    }

    private Collector getCollector() {
        return parameters.useBadCollector()
            ? Collectors.badCollector(new LoggingOutputStream(log), 0)
            : Collector.EMPTY;
    }

    private BatchImporter instantiateBatchImporter(
        GdsDatabaseLayout databaseLayout,
        LogService logService,
        Collector collector,
        JobScheduler jobScheduler
    ) {
        return Neo4jProxy.instantiateBatchImporter(
            databaseLayout,
            this.fileSystem,
            this.parameters.toBatchImporterConfig(),
            logService,
            this.executionMonitor,
            this.databaseConfig,
            jobScheduler,
            collector
        );
    }

    private boolean createAndStartDatabase() {
        var databaseName = parameters.dbName();
        dbms.createDatabase(databaseName);
        dbms.startDatabase(databaseName);

        var databaseService = dbms.database(databaseName);

        var numRetries = 10;
        for (int i = 0; i < numRetries; i++) {
            if (databaseService.isAvailable(1000)) {
                return true;
            }
            log.info(formatWithLocale("Database not available, retry %d of %d", i, numRetries));
        }
        return false;
    }
}
