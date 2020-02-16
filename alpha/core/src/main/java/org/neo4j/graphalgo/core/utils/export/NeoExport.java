/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.utils.export;

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.util.Validators;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.StoreLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.tooling.ImportTool;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.BatchImporterFactory;
import org.neo4j.unsafe.impl.batchimport.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors;

import java.io.File;
import java.io.IOException;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_internal_log_path;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;

public class NeoExport {

    private final Graph graph;

    private final NeoExportConfig config;

    public NeoExport(Graph graph, NeoExportConfig config) {
        this.graph = graph;
        this.config = config;
    }

    /**
     * @param defaultSettingsSuitableForTests default configuration geared towards unit/integration
     *      * test environments, for example lower default buffer sizes.
     */
    public void run(boolean defaultSettingsSuitableForTests) {
        File storeDir = new File(config.storeDir());
        Validators.DIRECTORY_IS_WRITABLE.validate(storeDir);

        DatabaseLayout databaseLayout = DatabaseLayout.of(storeDir);
        Config dbConfig = Config.defaults();
        File internalLogFile = dbConfig.get(store_internal_log_path);
        Configuration importConfig = ImportTool.importConfiguration(
            config.writeConcurrency(),
            defaultSettingsSuitableForTests,
            dbConfig,
            storeDir,
            false
        );

        LifeSupport life = new LifeSupport();

        try (FileSystemAbstraction fs = new DefaultFileSystemAbstraction()) {
            LogService logService = life.add(StoreLogService.withInternalLog(internalLogFile).build(fs));
            JobScheduler jobScheduler = life.add(createScheduler());

            life.start();

            Input input = new GraphInput(graph, config.batchSize());

            BatchImporter importer = BatchImporterFactory.withHighestPriority().instantiate(
                databaseLayout,
                fs,
                null, // no external page cache
                importConfig,
                logService,
                ExecutionMonitors.invisible(),
                EMPTY,
                dbConfig,
                RecordFormatSelector.selectForConfig(dbConfig, logService.getInternalLogProvider()),
                NO_MONITOR,
                jobScheduler
            );
            importer.doImport(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            life.shutdown();
        }
    }
}
