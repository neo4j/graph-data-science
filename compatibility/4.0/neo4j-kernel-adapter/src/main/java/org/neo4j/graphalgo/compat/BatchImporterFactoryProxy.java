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
package org.neo4j.graphalgo.compat;

import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.ImportLogic;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.staging.ExecutionMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodType.methodType;

final class BatchImporterFactoryProxy {

    private static final MethodHandle INSTANTIATE_BATCH_IMPORTER;

    static {
        MethodHandle instantiateBatchImporter;
        var lookup = MethodHandles.lookup();
        try {
            instantiateBatchImporter = neo405(lookup);
        } catch (ClassNotFoundException notOn405) {
            try {
                instantiateBatchImporter = neo404(lookup);
            } catch (ClassNotFoundException notOn404Either) {
                var error = new IllegalStateException(
                    "Could not load the correct LogFilesInitializer this is required to instantiate a BatchImporter."
                );
                error.addSuppressed(notOn405);
                error.addSuppressed(notOn404Either);
                instantiateBatchImporter = errorHandle(error);
            }
        }
        INSTANTIATE_BATCH_IMPORTER = instantiateBatchImporter;
    }

    static BatchImporter instantiateBatchImporter(
        BatchImporterFactory factory,
        DatabaseLayout directoryStructure,
        FileSystemAbstraction fileSystem,
        Configuration config,
        LogService logService,
        ExecutionMonitor executionMonitor,
        AdditionalInitialIds additionalInitialIds,
        Config dbConfig,
        RecordFormats recordFormats,
        ImportLogic.Monitor monitor,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        try {
            return (BatchImporter) INSTANTIATE_BATCH_IMPORTER.invoke(
                factory,
                directoryStructure,
                fileSystem,
                null,
                config,
                logService,
                executionMonitor,
                additionalInitialIds,
                dbConfig,
                recordFormats,
                monitor,
                jobScheduler,
                badCollector
            );
        } catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }


    private static MethodHandle neo404(MethodHandles.Lookup lookup) throws ClassNotFoundException {
        var interfaceClass = Class.forName("org.neo4j.internal.batchimport.LogFilesInitializer");
        var implClass = Class.forName("org.neo4j.batchinsert.internal.TransactionLogsInitializer");
        try {
            var logInitializer = lookup.findStaticGetter(implClass, "INSTANCE", interfaceClass).invoke();
            return fixBatchImporterApi(lookup, interfaceClass, logInitializer);
        } catch (Throwable error) {
            throw new IllegalStateException("Could not initialize the LogFilesInitializer", error);
        }
    }

    private static MethodHandle neo405(MethodHandles.Lookup lookup) throws ClassNotFoundException {
        var interfaceClass = Class.forName("org.neo4j.storageengine.api.LogFilesInitializer");
        var implClass = Class.forName("org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer");
        try {
            var logInitializer = lookup
                .findStatic(implClass, "getLogFilesInitializer", methodType(interfaceClass))
                .invoke();
            return fixBatchImporterApi(lookup, interfaceClass, logInitializer);
        } catch (Throwable error) {
            throw new IllegalStateException("Could not initialize the LogFilesInitializer", error);
        }
    }

    private static MethodHandle errorHandle(IllegalStateException error) {
        var biClass = BatchImporter.class;
        var throwError = MethodHandles.throwException(biClass, IllegalStateException.class);
        var throwingHandle = throwError.bindTo(error);
        return MethodHandles.dropArguments(
            throwingHandle,
            0,
            BatchImporterFactory.class,
            DatabaseLayout.class,
            FileSystemAbstraction.class,
            PageCache.class,
            Configuration.class,
            LogService.class,
            ExecutionMonitor.class,
            AdditionalInitialIds.class,
            Config.class,
            RecordFormats.class,
            ImportLogic.Monitor.class,
            JobScheduler.class,
            Collector.class
        );
    }

    private static MethodHandle fixBatchImporterApi(
        MethodHandles.Lookup lookup,
        Class<?> logFilesInitializerClass,
        Object logInitializer
    ) throws NoSuchMethodException, IllegalAccessException {
        var bifClass = BatchImporterFactory.class;
        var instantiateType = methodType(
            BatchImporter.class,
            DatabaseLayout.class,
            FileSystemAbstraction.class,
            PageCache.class,
            Configuration.class,
            LogService.class,
            ExecutionMonitor.class,
            AdditionalInitialIds.class,
            Config.class,
            RecordFormats.class,
            ImportLogic.Monitor.class,
            JobScheduler.class,
            Collector.class,
            logFilesInitializerClass
        );
        var instantiateMethod = lookup.findVirtual(
            bifClass,
            "instantiate",
            instantiateType
        );
        return insertArguments(instantiateMethod, instantiateType.parameterCount(), logInitializer);
    }

    private BatchImporterFactoryProxy() {}
}
