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
package org.neo4j.internal.recordstorage;

import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.migration.RollingUpgradeCompatibility;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public abstract class AbstractInMemoryStorageEngineFactory implements StorageEngineFactory {

    @Override
    public List<StoreMigrationParticipant> migrationParticipants(
        FileSystemAbstraction fs,
        Config config,
        PageCache pageCache,
        JobScheduler jobScheduler,
        LogService logService,
        PageCacheTracer cacheTracer,
        MemoryTracker memoryTracker
    ) {
        return List.of();
    }

    @Override
    public List<Path> listStorageFiles(
        FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout
    ) {
        return Collections.emptyList();
    }

    @Override
    public boolean storageExists(
        FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache
    ) {
        return false;
    }

    @Override
    public TransactionIdStore readOnlyTransactionIdStore(
        FileSystemAbstraction fileSystem,
        DatabaseLayout databaseLayout,
        PageCache pageCache,
        CursorContext cursorContext
    ) {
        return metadataProvider().transactionIdStore();
    }

    protected abstract AbstractInMemoryMetaDataProvider metadataProvider();

    @Override
    public LogVersionRepository readOnlyLogVersionRepository(
        DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext
    ) {
        return metadataProvider().logVersionRepository();
    }

    @Override
    public SchemaRuleMigrationAccess schemaRuleMigrationAccess(
        FileSystemAbstraction fs,
        PageCache pageCache,
        Config config,
        DatabaseLayout databaseLayout,
        LogService logService,
        String recordFormats,
        PageCacheTracer cacheTracer,
        CursorContext cursorContext,
        MemoryTracker memoryTracker
    ) {
        return new SchemaRuleMigrationAccess() {
            @Override
            public Iterable<SchemaRule> getAll() {
                return Collections.emptyList();
            }

            @Override
            public void writeSchemaRule(SchemaRule rule) {

            }

            @Override
            public void close() {

            }
        };
    }

    @Override
    public RollingUpgradeCompatibility rollingUpgradeCompatibility() {
        return null;
    }

    @Override
    public StoreId storeId(
        FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext
    ) {
        return StoreId.UNKNOWN;
    }

    @Override
    public void setStoreId(
        FileSystemAbstraction fs,
        DatabaseLayout databaseLayout,
        PageCache pageCache,
        CursorContext cursorContext,
        StoreId storeId,
        long upgradeTxChecksum,
        long upgradeTxCommitTimestamp
    ) throws IOException {
        MetaDataStore.setStoreId(pageCache,
            databaseLayout.metadataStore(),
            storeId,
            upgradeTxChecksum,
            upgradeTxCommitTimestamp,
            databaseLayout.getDatabaseName(),
            cursorContext
        );
    }

    @Override
    public Optional<UUID> databaseIdUuid(
        FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext
    ) {
        return MetaDataStore.getDatabaseIdUuid(
            pageCache,
            databaseLayout.metadataStore(),
            databaseLayout.getDatabaseName(),
            cursorContext
        );
    }

    @Override
    public List<SchemaRule> loadSchemaRules(
        FileSystemAbstraction fs,
        PageCache pageCache,
        Config config,
        DatabaseLayout databaseLayout,
        CursorContext cursorContext
    ) {
        return List.of();
    }

    @Override
    public StorageFilesState checkStoreFileState(
        FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache
    ) {
        return StorageFilesState.recoveredState();
    }

    @Override
    public CommandReaderFactory commandReaderFactory() {
        return InMemoryStorageCommandReaderFactory.INSTANCE;
    }
}
