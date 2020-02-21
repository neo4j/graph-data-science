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

import org.jetbrains.annotations.TestOnly;
import org.neo4j.common.Validator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.compat.SettingsProxy;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.EmptyLogFilesInitializer;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.staging.ExecutionMonitors;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.StoreLogService;
import org.neo4j.scheduler.JobScheduler;

import java.io.File;
import java.io.IOException;

import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;

public class NeoExport {

    private final Graph graph;

    private final NeoExportConfig config;

    public NeoExport(Graph graph, NeoExportConfig config) {
        this.graph = graph;
        this.config = config;
    }

    public void run() {
        run(false);
    }

    /**
     * Runs with default configuration geared towards
     * unit/integration test environments, for example,
     * lower default buffer sizes.
     */
    @TestOnly
    public void runFromTests() {
        run(true);
    }

    private void run(boolean defaultSettingsSuitableForTests) {
        File storeDir = new File(config.storeDir());
        DIRECTORY_IS_WRITABLE.validate(storeDir);

        Config dbConfig = Config.defaults(GraphDatabaseSettings.neo4j_home, storeDir.toPath());
        DatabaseLayout databaseLayout = DatabaseLayout.of(dbConfig);
        File internalLogFile = dbConfig.get(SettingsProxy.storeInternalLogPath()).toFile();
        // TODO: @s1ck ?????
        Configuration importConfig = new Configuration() {
            @Override
            public int maxNumberOfProcessors() {
                return config.writeConcurrency();
            }

            @Override
            public long pageCacheMemory() {
                return defaultSettingsSuitableForTests ? mebiBytes(8) : Configuration.super.pageCacheMemory();
            }

            @Override
            public boolean highIO() {
                return false;
            }
        };

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
                AdditionalInitialIds.EMPTY,
                dbConfig,
                RecordFormatSelector.selectForConfig(dbConfig, logService.getInternalLogProvider()),
                ImportLogic.NO_MONITOR,
                jobScheduler,
                Collector.EMPTY,
                EmptyLogFilesInitializer.INSTANCE
            );
            importer.doImport(input);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            life.shutdown();
        }
    }

    private static final Validator<File> DIRECTORY_IS_WRITABLE = value -> {
        if (value.mkdirs()) {   // It's OK, we created the directory right now, which means we have write access to it
            return;
        }

        File test = new File(value, "_______test___");
        try {
            test.createNewFile();
        } catch (IOException e) {
            throw new IllegalArgumentException("Directory '" + value + "' not writable: " + e.getMessage());
        } finally {
            test.delete();
        }
    };
}
