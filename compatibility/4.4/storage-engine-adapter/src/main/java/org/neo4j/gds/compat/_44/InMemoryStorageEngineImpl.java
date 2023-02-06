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

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.exceptions.KernelException;
import org.neo4j.gds.compat.TokenManager;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.storageengine.InMemoryDatabaseCreationCatalog;
import org.neo4j.gds.storageengine.InMemoryTransactionStateVisitor;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.recordstorage.InMemoryStorageReader44;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.InjectedNLIUpgradeCallback;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.Log;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandStream;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class InMemoryStorageEngineImpl implements StorageEngine {

    private final MetadataProvider metadataProvider;
    private final CypherGraphStore graphStore;
    private final DatabaseLayout databaseLayout;
    private final InMemoryTransactionStateVisitor txStateVisitor;
    private final TokenManager tokenManager;
    private final CountsStore countsStore;
    private final CommandCreationContext commandCreationContext;

    InMemoryStorageEngineImpl(
        DatabaseLayout databaseLayout,
        TokenHolders tokenHolders
    ) {
        this.databaseLayout = databaseLayout;
        this.graphStore = getGraphStoreFromCatalog(databaseLayout.getDatabaseName());
        this.txStateVisitor = new InMemoryTransactionStateVisitor(graphStore, tokenHolders);
        this.commandCreationContext = new InMemoryCommandCreationContextImpl();
        this.tokenManager = new TokenManager(
            tokenHolders,
            InMemoryStorageEngineImpl.this.txStateVisitor,
            InMemoryStorageEngineImpl.this.graphStore,
            newCommandCreationContext(EmptyMemoryTracker.INSTANCE)
        );

        InMemoryStorageEngineImpl.this.graphStore.initialize(tokenHolders);

        this.countsStore = new InMemoryCountsStore(graphStore, tokenHolders);
        this.metadataProvider = new InMemoryMetaDataProviderImpl();
    }

    private static CypherGraphStore getGraphStoreFromCatalog(String databaseName) {
        var graphName = InMemoryDatabaseCreationCatalog.getRegisteredDbCreationGraphName(databaseName);
        return (CypherGraphStore) GraphStoreCatalog.getAllGraphStores()
            .filter(graphStoreWithUserNameAndConfig -> graphStoreWithUserNameAndConfig
                .config()
                .graphName()
                .equals(graphName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(formatWithLocale(
                "No graph with name `%s` was found in GraphStoreCatalog. Available graph names are %s",
                graphName,
                GraphStoreCatalog.getAllGraphStores()
                    .map(GraphStoreCatalog.GraphStoreWithUserNameAndConfig::config)
                    .map(GraphProjectConfig::graphName)
                    .collect(Collectors.toList())
            )))
            .graphStore();
    }

    @Override
    public StoreEntityCounters storeEntityCounters() {
        return new StoreEntityCounters() {
            @Override
            public long nodes() {
                return graphStore.nodeCount();
            }

            @Override
            public long relationships() {
                return graphStore.relationshipCount();
            }

            @Override
            public long properties() {
                return graphStore.nodePropertyKeys().size() + graphStore.relationshipPropertyKeys().size();
            }

            @Override
            public long relationshipTypes() {
                return graphStore.relationshipTypes().size();
            }

            @Override
            public long allNodesCountStore(CursorContext cursorContext) {
                return graphStore.nodeCount();
            }

            @Override
            public long allRelationshipsCountStore(CursorContext cursorContext) {
                return graphStore.relationshipCount();
            }
        };
    }

    @Override
    public StoreCursors createStorageCursors(CursorContext initialContext) {
        return StoreCursors.NULL;
    }

    @Override
    public StorageLocks createStorageLocks(ResourceLocker locker) {
        return new InMemoryStorageLocksImpl(locker);
    }

    @Override
    public void createCommands(
        Collection<StorageCommand> target,
        ReadableTransactionState txState,
        StorageReader storageReader,
        CommandCreationContext creationContext,
        ResourceLocker locks,
        LockTracer lockTracer,
        long lastTransactionIdWhenStarted,
        TxStateVisitor.Decorator additionalTxStateVisitor,
        CursorContext cursorContext,
        StoreCursors storeCursors,
        MemoryTracker memoryTracker
    ) throws KernelException {
        txState.accept(txStateVisitor);
    }

    @Override
    public void forceClose() {
        shutdown();
    }

    @Override
    public void flushAndForce(CursorContext cursorContext) {

    }

    @Override
    public List<StorageCommand> createUpgradeCommands(
        KernelVersion versionToUpgradeTo, InjectedNLIUpgradeCallback injectedNLIUpgradeCallback
    ) {
        return null;
    }

    @Override
    public void dumpDiagnostics(Log errorLog, DiagnosticsLogger diagnosticsLog) {
    }

    @Override
    public StoreId getStoreId() {
        return metadataProvider.getStoreId();
    }

    @Override
    public StorageReader newReader() {
        return new InMemoryStorageReader44(graphStore, tokenManager.tokenHolders(), countsStore);
    }

    @Override
    public void addIndexUpdateListener(IndexUpdateListener listener) {

    }

    @Override
    public void apply(CommandsToApply batch, TransactionApplicationMode mode) {
    }

    @Override
    public void init() {
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        shutdown();
    }

    @Override
    public void shutdown() {
        InMemoryDatabaseCreationCatalog.removeDatabaseEntry(databaseLayout.getDatabaseName());
    }

    @Override
    public void listStorageFiles(
        Collection<StoreFileMetadata> atomic, Collection<StoreFileMetadata> replayable
    ) {

    }

    @Override
    public Lifecycle schemaAndTokensLifecycle() {
        return new LifecycleAdapter() {
            @Override
            public void init() {

            }
        };
    }

    @Override
    public CountsAccessor countsAccessor() {
        return countsStore;
    }

    @Override
    public MetadataProvider metadataProvider() {
        return metadataProvider;
    }

    @Override
    public CommandCreationContext newCommandCreationContext(MemoryTracker memoryTracker) {
        return commandCreationContext;
    }

    @Override
    public void lockRecoveryCommands(
        CommandStream commands, LockService lockService, LockGroup lockGroup, TransactionApplicationMode mode
    ) {

    }
}
