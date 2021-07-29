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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.configuration.Config;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.TriFunction;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.InjectedNLIUpgradeCallback;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.logging.Log;
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
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.neo4j.gds.storageengine.GraphStoreSettings.graph_name;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public abstract class AbstractInMemoryStorageEngine implements StorageEngine {

    private final TokenHolders tokenHolders;
    private final GraphStore graphStore;
    private final BiFunction<GraphStore, TokenHolders, TxStateVisitor> txStateVisitorFn;
    private final Supplier<CommandCreationContext> commandCreationContextSupplier;
    private final TriFunction<GraphStore, TokenHolders, CountsStore, StorageReader> storageReaderFn;
    private final CountsStore countsStore;
    private final MetadataProvider metadataProvider;

    public AbstractInMemoryStorageEngine(
        DatabaseLayout databaseLayout,
        TokenHolders tokenHolders,
        BiFunction<GraphStore, TokenHolders, CountsStore> countsStoreFn,
        BiFunction<GraphStore, TokenHolders, TxStateVisitor> txStateVisitorFn,
        MetadataProvider metadataProvider,
        Supplier<CommandCreationContext> commandCreationContextSupplier,
        TriFunction<GraphStore, TokenHolders, CountsStore, StorageReader> storageReaderFn,
        Config config
    ) {
        this.tokenHolders = tokenHolders;
        var graphName = config.get(graph_name);
        this.graphStore = GraphStoreCatalog.getAllGraphStores()
            .filter(graphStoreWithUserNameAndConfig -> graphStoreWithUserNameAndConfig
                .config()
                .graphName()
                .equals(graphName))
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException(formatWithLocale(
                "No graph with name %s was found in GraphStoreCatalog. Available graph names are %s",
                databaseLayout.getDatabaseName(),
                GraphStoreCatalog.getAllGraphStores()
                    .map(GraphStoreCatalog.GraphStoreWithUserNameAndConfig::config)
                    .map(GraphCreateConfig::graphName)
                    .collect(Collectors.toList())
            )))
            .graphStore();
        this.txStateVisitorFn = txStateVisitorFn;
        this.commandCreationContextSupplier = commandCreationContextSupplier;
        this.storageReaderFn = storageReaderFn;
        schemaAndTokensLifecycle();

        this.countsStore = countsStoreFn.apply(graphStore, tokenHolders);
        this.metadataProvider = metadataProvider;
    }

    protected void createCommands(ReadableTransactionState txState) throws KernelException {
        try (var txStateVisitor = txStateVisitorFn.apply(graphStore, tokenHolders)) {
            txState.accept(txStateVisitor);
        }
    }

    @Override
    public StorageReader newReader() {
        return storageReaderFn.apply(graphStore, tokenHolders, countsStore);
    }

    @Override
    public void addIndexUpdateListener(IndexUpdateListener listener) {

    }

    @Override
    public void apply(CommandsToApply batch, TransactionApplicationMode mode) {

    }

    @Override
    public void init() { }

    @Override
    public void start() {

    }

    @Override
    public void stop() {
        shutdown();
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void flushAndForce(CursorContext cursorContext) {

    }

    @Override
    public void forceClose() {
        try {
            shutdown();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
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
    public StoreId getStoreId() {
        return metadataProvider.getStoreId();
    }

    @Override
    public Lifecycle schemaAndTokensLifecycle() {
        MutableInt labelCounter = new MutableInt(0);
        MutableInt typeCounter = new MutableInt(0);
        MutableInt propertyCounter = new MutableInt(0);
        return new LifecycleAdapter() {
            @Override
            public void init() {
                graphStore
                    .nodePropertyKeys()
                    .values()
                    .stream()
                    .flatMap(Set::stream)
                    .distinct()
                    .forEach(propertyKey -> tokenHolders
                        .propertyKeyTokens()
                        .addToken(new NamedToken(propertyKey, propertyCounter.getAndIncrement())));

                graphStore
                    .relationshipPropertyKeys()
                    .forEach(propertyKey -> tokenHolders
                        .propertyKeyTokens()
                        .addToken(new NamedToken(propertyKey, propertyCounter.getAndIncrement())));

                graphStore
                    .nodeLabels()
                    .forEach(nodeLabel -> tokenHolders
                        .labelTokens()
                        .addToken(new NamedToken(nodeLabel.name(), labelCounter.getAndIncrement())));

                graphStore
                    .relationshipTypes()
                    .forEach(relType -> tokenHolders
                        .relationshipTypeTokens()
                        .addToken(new NamedToken(relType.name(), typeCounter.getAndIncrement())));
            }
        };
    }

    @Override
    public CountsAccessor countsAccessor() {
        return countsStore;
    }

    @Override
    public void dumpDiagnostics(Log errorLog, DiagnosticsLogger diagnosticsLog) {

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
}
