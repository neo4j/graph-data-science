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

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.migration.RollingUpgradeCompatibility;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

public abstract class AbstractInMemoryStorageEngineFactory implements StorageEngineFactory {

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

    protected abstract SchemaRuleMigrationAccess schemaRuleMigrationAccess();

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
    public StorageFilesState checkStoreFileState(
        FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache
    ) {
        return StorageFilesState.recoveredState();
    }
}
