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
package org.neo4j.gds.compat.dev;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.storageengine.InMemoryTransactionStateVisitor;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.batchimport.AdditionalInitialIds;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.Configuration;
import org.neo4j.internal.batchimport.IndexConfig;
import org.neo4j.internal.batchimport.IndexImporterFactory;
import org.neo4j.internal.batchimport.Monitor;
import org.neo4j.internal.batchimport.ReadBehaviour;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.LenientStoreInput;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory;
import org.neo4j.internal.recordstorage.AbstractInMemoryMetaDataProvider;
import org.neo4j.internal.recordstorage.AbstractInMemoryStorageEngineFactory;
import org.neo4j.internal.recordstorage.InMemoryStorageCommandReaderFactory;
import org.neo4j.internal.recordstorage.InMemoryStorageReaderDev;
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
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.lock.LockService;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.ConstraintRuleAccessor;
import org.neo4j.storageengine.api.LogFilesInitializer;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.token.DelegatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokensLoader;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.readOnly;

@ServiceProvider
public class InMemoryStorageEngineFactoryDev extends AbstractInMemoryStorageEngineFactory {

    public static final String IN_MEMORY_STORAGE_ENGINE_NAME_DEV = "in-memory-dev";

    private final AbstractInMemoryMetaDataProvider metadataProvider = new InMemoryMetaDataProviderImpl();

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
            .withStorageReaderFn(InMemoryStorageReaderDev::new)
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
        MetaDataStore.getDatabaseIdUuid(pageCache, RecordDatabaseLayout.convert(databaseLayout).metadataStore(), databaseLayout.getDatabaseName(), cursorContext);
    }

    @Override
    public DatabaseLayout databaseLayout(
        Neo4jLayout neo4jLayout, String databaseName
    ) {
        return RecordDatabaseLayout.of(neo4jLayout, databaseName);
    }

    @Override
    public IndexConfig matchingBatchImportIndexConfiguration(
        FileSystemAbstraction fileSystemAbstraction, DatabaseLayout databaseLayout, PageCache pageCache
    ) {
        return new IndexConfig();
    }

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
        MemoryTracker memoryTracker
    ) {
        return Neo4jProxy.instantiateBatchImporter(
            BatchImporterFactory.withHighestPriority(),
            databaseLayout,
            fileSystemAbstraction,
            pageCacheTracer,
            configuration.maxNumberOfProcessors(),
            Optional.of(configuration.pageCacheMemory()),
            logService,
            Neo4jProxy.invisibleExecutionMonitor(),
            AdditionalInitialIds.EMPTY,
            config,
            RecordFormatSelector.selectForConfig(config, logService.getInternalLogProvider()),
            jobScheduler,
            Collector.EMPTY
        );
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
        boolean b
    ) {
        NeoStores neoStores = (new StoreFactory(databaseLayout, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, fileSystemAbstraction, NullLogProvider.getInstance(), pageCacheTracer, readOnly())).openAllNeoStores();
        return new LenientStoreInput(neoStores, readBehaviour.decorateTokenHolders(this.loadReadOnlyTokens(neoStores, true, PageCacheTracer.NULL)), true, pageCacheTracer, readBehaviour);
    }

    @Override
    public String name() {
        return IN_MEMORY_STORAGE_ENGINE_NAME_DEV;
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
    public StoreVersion defaultStoreVersion() {
        return new InMemoryStoreVersion();
    }

    @Override
    protected AbstractInMemoryMetaDataProvider metadataProvider() {
        return metadataProvider;
    }

    @Override
    public List<SchemaRule> loadSchemaRules(
        FileSystemAbstraction fileSystemAbstraction,
        PageCache pageCache,
        Config config,
        DatabaseLayout databaseLayout,
        boolean b,
        Function<SchemaRule, SchemaRule> function,
        PageCacheTracer pageCacheTracer
    ) {
        return List.of();
    }

    @Override
    public TokenHolders loadReadOnlyTokens( FileSystemAbstraction fs, DatabaseLayout layout, Config config, PageCache pageCache, boolean lenient,
                                            PageCacheTracer pageCacheTracer )
    {
        StoreFactory factory =
            new StoreFactory( layout, config, new ScanOnOpenReadOnlyIdGeneratorFactory(), pageCache, fs,
                NullLogProvider.getInstance(), pageCacheTracer, readOnly() );
        try ( NeoStores stores = factory.openNeoStores( false,
            StoreType.PROPERTY_KEY_TOKEN, StoreType.PROPERTY_KEY_TOKEN_NAME,
            StoreType.LABEL_TOKEN, StoreType.LABEL_TOKEN_NAME,
            StoreType.RELATIONSHIP_TYPE_TOKEN, StoreType.RELATIONSHIP_TYPE_TOKEN_NAME ) )
        {
            return loadReadOnlyTokens( stores, lenient, pageCacheTracer );
        }
    }

    private TokenHolders loadReadOnlyTokens( NeoStores stores, boolean lenient, PageCacheTracer pageCacheTracer )
    {
        try ( var cursorTracer = pageCacheTracer.createPageCursorTracer( "loadReadOnlyTokens" );
              var cursorContext = new CursorContext( cursorTracer );
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
        return InMemoryStorageCommandReaderFactory.INSTANCE;
    }
}
