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
package org.neo4j.gds.compat._520;

import org.neo4j.configuration.Config;
import org.neo4j.gds.compat.CompatMonitor;
import org.neo4j.gds.compat.Neo4jProxyApi;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;

import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class Neo4jProxyImpl implements Neo4jProxyApi {

    public CursorContextFactory cursorContextFactory(Optional<PageCacheTracer> pageCacheTracer) {
        return pageCacheTracer.map(cacheTracer -> new CursorContextFactory(
            cacheTracer,
            FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER
        )).orElse(CursorContextFactory.NULL_CONTEXT_FACTORY);
    }

    @Override
    public long estimateNodeCount(Read read, int label) {
        return read.estimateCountsForNode(label);
    }

    @Override
    public long estimateRelationshipCount(Read read, int sourceLabel, int targetLabel, int type) {
        return read.estimateCountsForRelationships(sourceLabel, type, targetLabel);
    }

    @Override
    public BatchImporter instantiateBlockBatchImporter(
        DatabaseLayout databaseLayout,
        FileSystemAbstraction fileSystem,
        PageCacheTracer pageCacheTracer,
        Configuration configuration,
        CompatMonitor compatMonitor,
        LogService logService,
        AdditionalInitialIds additionalInitialIds,
        Config dbConfig,
        JobScheduler jobScheduler,
        Collector badCollector
    ) {
        var storageEngineFactory = StorageEngineFactory.selectStorageEngine(dbConfig);
        var progressOutput = new PrintStream(PrintStream.nullOutputStream(), true, StandardCharsets.UTF_8);
        var verboseProgressOutput = false;

        return storageEngineFactory.batchImporter(
            databaseLayout,
            fileSystem,
            pageCacheTracer,
            configuration,
            logService,
            progressOutput,
            verboseProgressOutput,
            additionalInitialIds,
            dbConfig,
            toMonitor(compatMonitor),
            jobScheduler,
            badCollector,
            TransactionLogInitializer.getLogFilesInitializer(),
            new IndexImporterFactoryImpl(),
            EmptyMemoryTracker.INSTANCE,
            cursorContextFactory(Optional.empty())
        );
    }

    private static Monitor toMonitor(CompatMonitor compatMonitor) {
        return new Monitor() {
            @Override
            public void started() {
                compatMonitor.started();
            }

            @Override
            public void percentageCompleted(int percentage) {
                compatMonitor.percentageCompleted(percentage);
            }

            @Override
            public void completed(boolean success) {
                compatMonitor.completed(success);
            }
        };
    }
}
