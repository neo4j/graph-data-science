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
package org.neo4j.gds.compat._56;

import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.report.ConsistencySummaryStatistics;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.gds.annotation.SuppressForbidden;
import org.neo4j.gds.compat.Neo4jVersion;
import org.neo4j.gds.compat.StorageEngineProxyApi;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IncrementalBatchImporter;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.ReadBehaviour;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.LenientStoreInput;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.internal.recordstorage.InMemoryStorageCommandReaderFactory56;
import org.neo4j.internal.recordstorage.StoreTokens;
import org.neo4j.internal.schema.IndexConfigCompleter;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionRepository;
import org.neo4j.kernel.api.index.IndexProvidersAccess;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.lock.LockService;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.SchemaRule44;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StorageFilesState;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.StoreVersionIdentifier;
import org.neo4j.storageengine.migration.StoreMigrationParticipant;
import org.neo4j.time.SystemNanoClock;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokensLoader;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

@ServiceProvider
public class InMemoryStorageEngineFactory implements StorageEngineFactory {

    static final String IN_MEMORY_STORAGE_ENGINE_NAME = "in-memory-rc";

    public InMemoryStorageEngineFactory() {
        StorageEngineProxyApi.requireNeo4jVersion(Neo4jVersion.V_5_6, StorageEngineFactory.class);
    }

    // Record storage = 0, Freki = 1
    // Let's leave some room for future storage engines
    // This arbitrary seems quite future-proof
    public static final byte ID = 42;

    @Override
    public byte id() {
        return ID;
    }

    @Override
    public boolean storageExists(FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout) {
        return false;
    }

    @Override
    public StorageEngine instantiate(
        FileSystemAbstraction fs,
        Clock clock,
        DatabaseLayout databaseLayout,
        Config config,
        PageCache pageCache,
        TokenHolders tokenHolders,
        SchemaState schemaState,
        ConstraintRuleAccessor constraintSemantics,
        IndexConfigCompleter indexConfigCompleter,
        LockService lockService,
        IdGeneratorFactory idGeneratorFactory,
        DatabaseHealth databaseHealth,
        InternalLogProvider internalLogProvider,
        InternalLogProvider userLogProvider,
        RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
        LogTailMetadata logTailMetadata,
        KernelVersionRepository kernelVersionRepository,
        MemoryTracker memoryTracker,
        CursorContextFactory contextFactory,
        PageCacheTracer pageCacheTracer
    ) {
        StoreFactory factory = new StoreFactory(
            databaseLayout,
            config,
            idGeneratorFactory,
            pageCache,
            pageCacheTracer,
            fs,
            internalLogProvider,
            contextFactory,
            false,
            logTailMetadata
        );

        factory.openNeoStores(StoreType.LABEL_TOKEN).close();

        return new InMemoryStorageEngineImpl(
            databaseLayout,
            tokenHolders
        );
    }

    @Override
    public Optional<UUID> databaseIdUuid(
        FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache, CursorContext cursorContext
    ) {
        var fieldAccess = MetaDataStore.getFieldAccess(
            pageCache,
            RecordDatabaseLayout.convert(databaseLayout).metadataStore(),
            databaseLayout.getDatabaseName(),
            cursorContext
        );

        try {
            return fieldAccess.readDatabaseUUID();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<StoreMigrationParticipant> migrationParticipants(
        FileSystemAbstraction fileSystemAbstraction,
        Config config,
        PageCache pageCache,
        JobScheduler jobScheduler,
        LogService logService,
        MemoryTracker memoryTracker,
        PageCacheTracer pageCacheTracer,
        CursorContextFactory cursorContextFactory,
        boolean b
    ) {
        return List.of();
    }

    @Override
    public DatabaseLayout databaseLayout(
        Neo4jLayout neo4jLayout, String databaseName
    ) {
        return RecordDatabaseLayout.of(neo4jLayout, databaseName);
    }

    @Override
    public DatabaseLayout formatSpecificDatabaseLayout(DatabaseLayout plainLayout) {
        return databaseLayout(plainLayout.getNeo4jLayout(), plainLayout.getDatabaseName());
    }

    @SuppressForbidden(reason = "This is the compat layer and we don't really need to go through the proxy")
    @Override
    public BatchImporter batchImporter(
        DatabaseLayout databaseLayout,
        FileSystemAbstraction fileSystemAbstraction,
        PageCacheTracer pageCacheTracer,
        Configuration configuration,
        LogService logService,
        PrintStream printStream,
        boolean b,
        AdditionalInitialIds additionalInitialIds,
        Config config,
        Monitor monitor,
        JobScheduler jobScheduler,
        Collector collector,
        LogFilesInitializer logFilesInitializer,
        IndexImporterFactory indexImporterFactory,
        MemoryTracker memoryTracker,
        CursorContextFactory cursorContextFactory
    ) {
        throw new UnsupportedOperationException("Batch Import into GDS is not supported");
    }

    @Override
    public Input asBatchImporterInput(
        DatabaseLayout databaseLayout,
        FileSystemAbstraction fileSystemAbstraction,
        PageCache pageCache,
        PageCacheTracer pageCacheTracer,
        Config config,
        MemoryTracker memoryTracker,
        ReadBehaviour readBehaviour,
        boolean b,
        CursorContextFactory cursorContextFactory,
        LogTailMetadata logTailMetadata
    ) {
        NeoStores neoStores = (new StoreFactory(
            databaseLayout,
            config,
            new ScanOnOpenReadOnlyIdGeneratorFactory(),
            pageCache,
            pageCacheTracer,
            fileSystemAbstraction,
            NullLogProvider.getInstance(),
            cursorContextFactory,
            false,
            logTailMetadata
        )).openAllNeoStores();
        return new LenientStoreInput(
            neoStores,
            readBehaviour.decorateTokenHolders(this.loadReadOnlyTokens(neoStores, true, cursorContextFactory)),
            true,
            cursorContextFactory,
            readBehaviour
        );
    }

    @Override
    public long optimalAvailableConsistencyCheckerMemory(
        FileSystemAbstraction fileSystemAbstraction,
        DatabaseLayout databaseLayout,
        Config config,
        PageCache pageCache
    ) {
        return 0;
    }

    @Override
    public String name() {
        return IN_MEMORY_STORAGE_ENGINE_NAME;
    }

    @Override
    public Set<String> supportedFormats(boolean includeFormatsUnderDevelopment) {
        return Set.of(IN_MEMORY_STORAGE_ENGINE_NAME);
    }

    @Override
    public boolean supportedFormat(String format, boolean includeFormatsUnderDevelopment) {
        return format.equals(IN_MEMORY_STORAGE_ENGINE_NAME);
    }

    @Override
    public MetadataProvider transactionMetaDataStore(
        FileSystemAbstraction fs,
        DatabaseLayout databaseLayout,
        Config config,
        PageCache pageCache,
        DatabaseReadOnlyChecker readOnlyChecker,
        CursorContextFactory contextFactory,
        LogTailLogVersionsMetadata logTailMetadata,
        PageCacheTracer pageCacheTracer
    ) {
        return new InMemoryMetaDataProviderImpl();
    }

    @Override
    public StoreVersionCheck versionCheck(
        FileSystemAbstraction fileSystemAbstraction,
        DatabaseLayout databaseLayout,
        Config config,
        PageCache pageCache,
        LogService logService,
        CursorContextFactory cursorContextFactory
    ) {
        return new InMemoryVersionCheck();
    }

    @Override
    public List<SchemaRule> loadSchemaRules(
        FileSystemAbstraction fileSystemAbstraction,
        PageCache pageCache,
        PageCacheTracer pageCacheTracer,
        Config config,
        DatabaseLayout databaseLayout,
        boolean b,
        Function<SchemaRule, SchemaRule> function,
        CursorContextFactory cursorContextFactory
    ) {
        return List.of();
    }

    @Override
    public List<SchemaRule44> load44SchemaRules(
        FileSystemAbstraction fs,
        PageCache pageCache,
        PageCacheTracer pageCacheTracer,
        Config config,
        DatabaseLayout databaseLayout,
        CursorContextFactory contextFactory,
        LogTailLogVersionsMetadata logTailMetadata
    ) {
        return List.of();
    }

    @Override
    public TokenHolders loadReadOnlyTokens(
        FileSystemAbstraction fileSystemAbstraction,
        DatabaseLayout databaseLayout,
        Config config,
        PageCache pageCache,
        PageCacheTracer pageCacheTracer,
        boolean lenient,
        CursorContextFactory cursorContextFactory
    ) {
        StoreFactory factory = new StoreFactory(
            databaseLayout,
            config,
            new ScanOnOpenReadOnlyIdGeneratorFactory(),
            pageCache,
            pageCacheTracer,
            fileSystemAbstraction,
            NullLogProvider.getInstance(),
            cursorContextFactory,
            false,
            LogTailMetadata.EMPTY_LOG_TAIL
        );
        try ( NeoStores stores = factory.openNeoStores(
            StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME,
            StoreType.LABEL_TOKEN, StoreType.LABEL_TOKEN_NAME,
            StoreType.RELATIONSHIP_TYPE_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN_NAME ) )
        {
            return loadReadOnlyTokens(stores, lenient, cursorContextFactory);
        }
    }

    private TokenHolders loadReadOnlyTokens(
        NeoStores stores,
        boolean lenient,
        CursorContextFactory cursorContextFactory
    )
    {
        try ( var cursorContext = cursorContextFactory.create("loadReadOnlyTokens");
              var storeCursors = new CachedStoreCursors( stores, cursorContext ) )
        {
            stores.start( cursorContext );
            TokensLoader loader = lenient ? StoreTokens.allReadableTokens( stores ) : StoreTokens.allTokens( stores );
            TokenHolder propertyKeys = new DelegatingTokenHolder( ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_PROPERTY_KEY );
            TokenHolder labels = new DelegatingTokenHolder( ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_LABEL );
            TokenHolder relationshipTypes = new DelegatingTokenHolder( ReadOnlyTokenCreator.READ_ONLY, TokenHolder.TYPE_RELATIONSHIP_TYPE );

            propertyKeys.setInitialTokens( lenient ? unique( loader.getPropertyKeyTokens( storeCursors ) ) : loader.getPropertyKeyTokens( storeCursors ) );
            labels.setInitialTokens( lenient ? unique( loader.getLabelTokens( storeCursors ) ) : loader.getLabelTokens( storeCursors ) );
            relationshipTypes.setInitialTokens(
                lenient ? unique( loader.getRelationshipTypeTokens( storeCursors ) ) : loader.getRelationshipTypeTokens( storeCursors ) );
            return new TokenHolders( propertyKeys, labels, relationshipTypes );
        }
        catch ( IOException e )
        {
            throw new UncheckedIOException( e );
        }
    }

    private static List<NamedToken> unique( List<NamedToken> tokens )
    {
        if ( !tokens.isEmpty() )
        {
            Set<String> names = new HashSet<>( tokens.size() );
            int i = 0;
            while ( i < tokens.size() )
            {
                if ( names.add( tokens.get( i ).name() ) )
                {
                    i++;
                }
                else
                {
                    // Remove the token at the given index, by replacing it with the last token in the list.
                    // This changes the order of elements, but can be done in constant time instead of linear time.
                    int lastIndex = tokens.size() - 1;
                    NamedToken endToken = tokens.remove( lastIndex );
                    if ( i < lastIndex )
                    {
                        tokens.set( i, endToken );
                    }
                }
            }
        }
        return tokens;
    }

    @Override
    public CommandReaderFactory commandReaderFactory() {
        return InMemoryStorageCommandReaderFactory56.INSTANCE;
    }

    @Override
    public void consistencyCheck(
        FileSystemAbstraction fileSystem,
        DatabaseLayout layout,
        Config config,
        PageCache pageCache,
        IndexProviderMap indexProviders,
        InternalLog log,
        ConsistencySummaryStatistics summary,
        int numberOfThreads,
        long maxOffHeapCachingMemory,
        OutputStream progressOutput,
        boolean verbose,
        ConsistencyFlags flags,
        CursorContextFactory contextFactory,
        PageCacheTracer pageCacheTracer,
        LogTailMetadata logTailMetadata
    ) {
        // we can do no-op, since our "database" is _always_ consistent
    }

    @Override
    public ImmutableSet<OpenOption> getStoreOpenOptions(
        FileSystemAbstraction fs,
        PageCache pageCache,
        DatabaseLayout layout,
        CursorContextFactory contextFactory
    ) {
        // Not sure about this, empty set is returned when the store files are in `little-endian` format
        // See: `org.neo4j.kernel.impl.store.format.PageCacheOptionsSelector.select`
        return Sets.immutable.empty();
    }

    @Override
    public StoreId retrieveStoreId(
        FileSystemAbstraction fs,
        DatabaseLayout databaseLayout,
        PageCache pageCache,
        CursorContext cursorContext
    ) throws IOException {
        return StoreId.retrieveFromStore(fs, databaseLayout, pageCache, cursorContext);
    }


    @Override
    public Optional<StoreVersion> versionInformation(StoreVersionIdentifier storeVersionIdentifier) {
        return Optional.of(new InMemoryStoreVersion());
    }

    @Override
    public void resetMetadata(
        FileSystemAbstraction fileSystemAbstraction,
        DatabaseLayout databaseLayout,
        Config config,
        PageCache pageCache,
        CursorContextFactory cursorContextFactory,
        PageCacheTracer pageCacheTracer,
        StoreId storeId,
        UUID externalStoreId
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IncrementalBatchImporter incrementalBatchImporter(
        DatabaseLayout databaseLayout,
        FileSystemAbstraction fileSystem,
        PageCacheTracer pageCacheTracer,
        Configuration config,
        LogService logService,
        PrintStream progressOutput,
        boolean verboseProgressOutput,
        AdditionalInitialIds additionalInitialIds,
        ThrowingSupplier<LogTailMetadata, IOException> logTailMetadataSupplier,
        Config dbConfig,
        Monitor monitor,
        JobScheduler jobScheduler,
        Collector badCollector,
        IndexImporterFactory indexImporterFactory,
        MemoryTracker memoryTracker,
        CursorContextFactory contextFactory,
        IndexProvidersAccess indexProvidersAccess
    ) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Locks createLocks(Config config, SystemNanoClock clock) {
        return Locks.NO_LOCKS;
    }

    @Override
    public List<Path> listStorageFiles(
        FileSystemAbstraction fileSystem, DatabaseLayout databaseLayout
    ) {
        return Collections.emptyList();
    }

    @Override
    public StorageFilesState checkStoreFileState(
        FileSystemAbstraction fs, DatabaseLayout databaseLayout, PageCache pageCache
    ) {
        return StorageFilesState.recoveredState();
    }
}
