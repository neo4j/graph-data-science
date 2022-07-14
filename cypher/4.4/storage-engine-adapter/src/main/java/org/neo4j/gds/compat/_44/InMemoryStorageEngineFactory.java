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
package org.neo4j.gds.compat._44;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.gds.storageengine.InMemoryTransactionStateVisitor;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.recordstorage.AbstractInMemoryMetaDataProvider;
import org.neo4j.internal.recordstorage.AbstractInMemoryStorageEngineFactory;
import org.neo4j.internal.recordstorage.InMemoryStorageCommandReaderFactory;
import org.neo4j.internal.recordstorage.InMemoryStorageReader44;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.storageengine.migration.RollingUpgradeCompatibility;
import org.neo4j.storageengine.migration.SchemaRuleMigrationAccess;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.token.TokenHolders;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@ServiceProvider
public class InMemoryStorageEngineFactory extends AbstractInMemoryStorageEngineFactory {

    static final String IN_MEMORY_STORAGE_ENGINE_NAME = "in-memory-44";

    private final AbstractInMemoryMetaDataProvider metadataProvider = new InMemoryMetaDataProviderImpl();

    @Override
    public boolean storageExists(
        FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout, PageCache pageCache
    ) {
        return false;
    }

    @Override
    public RollingUpgradeCompatibility rollingUpgradeCompatibility() {
        return null;
    }

    @Override
    public StorageEngine instantiate(
        FileSystemAbstraction fs,
        DatabaseLayout databaseLayout,
        Config config,
        PageCache pageCache,
        TokenHolders tokenHolders,
        SchemaState schemaState,
        ConstraintRuleAccessor constraintSemantics,
        IndexConfigCompleter indexConfigCompleter,
        LockService lockService,
        IdGeneratorFactory idGeneratorFactory,
        IdController idController,
        DatabaseHealth databaseHealth,
        LogProvider internalLogProvider,
        LogProvider userLogProvider,
        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
        PageCacheTracer cacheTracer,
        boolean createStoreIfNotExists,
        DatabaseReadOnlyChecker readOnlyChecker,
        MemoryTracker memoryTracker
    ) {
        StoreFactory factory = new StoreFactory(
            databaseLayout,
            config,
            idGeneratorFactory,
            pageCache,
            fs,
            internalLogProvider,
            cacheTracer,
            readOnlyChecker
        );

        factory.openNeoStores(createStoreIfNotExists, StoreType.LABEL_TOKEN).close();

        return new InMemoryStorageEngineImpl.Builder(databaseLayout, tokenHolders, metadataProvider)
            .withCommandCreationContextSupplier(InMemoryCommandCreationContextImpl::new)
            .withStorageReaderFn(InMemoryStorageReader44::new)
            .withTxStateVisitorFn(InMemoryTransactionStateVisitor::new)
            .withCountsStoreFn(InMemoryCountsStoreImpl::new)
            .withCommandCreationContextSupplier(InMemoryCommandCreationContextImpl::new)
            .build();
    }

    @Override
    public void setExternalStoreUUID(
        FileSystemAbstraction fileSystemAbstraction,
        DatabaseLayout databaseLayout,
        PageCache pageCache,
        CursorContext cursorContext,
        UUID uuid
    ) {
        MetaDataStore.getDatabaseIdUuid(
            pageCache,
            RecordDatabaseLayout.convert(databaseLayout).metadataStore(),
            databaseLayout.getDatabaseName(),
            cursorContext
        );
    }

    @Override
    public Optional<UUID> databaseIdUuid(
        FileSystemAbstraction fs,
        DatabaseLayout databaseLayout,
        PageCache pageCache,
        CursorContext cursorContext
    ) {
        return MetaDataStore.getDatabaseIdUuid(
            pageCache,
            databaseLayout.metadataStore(),
            databaseLayout.getDatabaseName(),
            cursorContext
        );
    }

    @Override
    public DatabaseLayout databaseLayout(
        Neo4jLayout neo4jLayout, String databaseName
    ) {
        return RecordDatabaseLayout.of(neo4jLayout, databaseName);
    }

    @Override
    public String name() {
        return IN_MEMORY_STORAGE_ENGINE_NAME;
    }

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
    public MetadataProvider transactionMetaDataStore(
        FileSystemAbstraction fs,
        DatabaseLayout databaseLayout,
        Config config,
        PageCache pageCache,
        PageCacheTracer cacheTracer,
        DatabaseReadOnlyChecker readOnlyChecker
    ) {
        return metadataProvider();
    }

    @Override
    public StoreVersionCheck versionCheck(
        FileSystemAbstraction fs,
        DatabaseLayout databaseLayout,
        Config config,
        PageCache pageCache,
        LogService logService,
        PageCacheTracer pageCacheTracer
    ) {
        return new InMemoryVersionCheck();
    }

    @Override
    public StoreVersion versionInformation(String storeVersion) {
        return new InMemoryStoreVersion();
    }

    @Override
    public StoreVersion versionInformation(StoreId storeId) {
        return new InMemoryStoreVersion();
    }

    @Override
    protected AbstractInMemoryMetaDataProvider metadataProvider() {
        return metadataProvider;
    }

    @Override
    protected SchemaRuleMigrationAccess schemaRuleMigrationAccess() {
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
    public CommandReaderFactory commandReaderFactory() {
        return InMemoryStorageCommandReaderFactory.INSTANCE;
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
        return schemaRuleMigrationAccess();
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

    @Override
    public LogVersionRepository readOnlyLogVersionRepository(
        DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext
    ) {
        return metadataProvider.logVersionRepository();
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
    public StoreId storeId(
        FileSystemAbstraction fs,
        DatabaseLayout databaseLayout,
        PageCache pageCache,
        CursorContext cursorContext
    ) {
        return metadataProvider.getStoreId();
    }
}
