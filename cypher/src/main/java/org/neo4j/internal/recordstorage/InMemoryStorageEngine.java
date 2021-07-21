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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.gds.storageengine.InMemoryCountStore;
import org.neo4j.gds.storageengine.InMemoryMetaDataProvider;
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
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
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
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class InMemoryStorageEngine implements StorageEngine, Lifecycle {

    private final TokenHolders tokenHolders;
    private final InMemoryMetaDataProvider metaDataProvider;
    private final GraphStore graphStore;
    private final InMemoryCountStore countStore;

    public InMemoryStorageEngine(
        DatabaseLayout databaseLayout,
        TokenHolders tokenHolders,
        InMemoryMetaDataProvider metaDataProvider
    ) {
        this.tokenHolders = tokenHolders;
        this.metaDataProvider = metaDataProvider;

        this.graphStore = GraphStoreCatalog.getAllGraphStores()
            .filter(graphStoreWithUserNameAndConfig -> graphStoreWithUserNameAndConfig.config().graphName().equals(databaseLayout.getDatabaseName()))
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
        schemaAndTokensLifecycle();

        this.countStore = new InMemoryCountStore(graphStore, tokenHolders);
    }

    @Override
    public StorageReader newReader() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addIndexUpdateListener(IndexUpdateListener listener) {

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
        MemoryTracker memoryTracker
    ) {
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
        return metaDataProvider.getStoreId();
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
        return countStore;
    }

    @Override
    public void dumpDiagnostics(Log errorLog, DiagnosticsLogger diagnosticsLog) {

    }

    @Override
    public MetadataProvider metadataProvider() {
        return metaDataProvider;
    }

    @Override
    public CommandCreationContext newCommandCreationContext(MemoryTracker memoryTracker) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void lockRecoveryCommands(
        CommandStream commands, LockService lockService, LockGroup lockGroup, TransactionApplicationMode mode
    ) {

    }
}
