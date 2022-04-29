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
package org.neo4j.gds.compat;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.TriFunction;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.storageengine.InMemoryDatabaseCreationCatalog;
import org.neo4j.gds.storageengine.InMemoryTransactionStateVisitor;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.InjectedNLIUpgradeCallback;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.CommandStream;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.token.TokenHolders;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class AbstractInMemoryStorageEngine implements StorageEngine {

    private final DatabaseLayout databaseLayout;
    private final InMemoryTransactionStateVisitor txStateVisitor;
    private final Supplier<CommandCreationContext> commandCreationContextSupplier;
    private final TriFunction<CypherGraphStore, TokenHolders, CountsStore, StorageReader> storageReaderFn;
    private final TokenManager tokenManager;
    private final CountsStore countsStore;
    protected final MetadataProvider metadataProvider;
    protected final CypherGraphStore graphStore;

    public AbstractInMemoryStorageEngine(
        DatabaseLayout databaseLayout,
        TokenHolders tokenHolders,
        BiFunction<GraphStore, TokenHolders, CountsStore> countsStoreFn,
        BiFunction<CypherGraphStore, TokenHolders, InMemoryTransactionStateVisitor> txStateVisitorFn,
        MetadataProvider metadataProvider,
        Supplier<CommandCreationContext> commandCreationContextSupplier,
        TriFunction<CypherGraphStore, TokenHolders, CountsStore, StorageReader> storageReaderFn
    ) {
        this.databaseLayout = databaseLayout;
        this.graphStore = getGraphStoreFromCatalog(databaseLayout.getDatabaseName());
        this.txStateVisitor = txStateVisitorFn.apply(graphStore, tokenHolders);
        this.commandCreationContextSupplier = commandCreationContextSupplier;
        this.storageReaderFn = storageReaderFn;
        this.tokenManager = new TokenManager(
            tokenHolders,
            txStateVisitor,
            graphStore,
            newCommandCreationContext(EmptyMemoryTracker.INSTANCE)
        );
        graphStore.initialize(tokenHolders);

        this.countsStore = countsStoreFn.apply(graphStore, tokenHolders);
        this.metadataProvider = metadataProvider;
    }

    protected void createCommands(ReadableTransactionState txState) throws KernelException {
        txState.accept(txStateVisitor);
    }

    @Override
    public StorageReader newReader() {
        return storageReaderFn.apply(graphStore, tokenManager.tokenHolders(), countsStore);
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
    public void flushAndForce(CursorContext cursorContext) {

    }

    @Override
    public List<StorageCommand> createUpgradeCommands(
        KernelVersion versionToUpgradeTo, InjectedNLIUpgradeCallback injectedNLIUpgradeCallback
    ) {
        return null;
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
        return commandCreationContextSupplier.get();
    }

    @Override
    public void lockRecoveryCommands(
        CommandStream commands, LockService lockService, LockGroup lockGroup, TransactionApplicationMode mode
    ) {

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
}
