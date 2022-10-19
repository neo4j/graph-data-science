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
package org.neo4j.gds.compat._51;

import org.neo4j.counts.CountsStore;
import org.neo4j.exceptions.KernelException;
import org.neo4j.function.TriFunction;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.compat.AbstractInMemoryStorageEngine;
import org.neo4j.gds.compat.InMemoryStorageEngineBuilder;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.storageengine.InMemoryTransactionStateVisitor;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.store.stats.StoreEntityCounters;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageEngineIndexingBehaviour;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;
import org.neo4j.storageengine.api.txstate.TxStateVisitor;
import org.neo4j.token.TokenHolders;

import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;

public final class InMemoryStorageEngineImpl extends AbstractInMemoryStorageEngine {

    private InMemoryStorageEngineImpl(
        DatabaseLayout databaseLayout,
        TokenHolders tokenHolders,
        BiFunction<GraphStore, TokenHolders, CountsStore> countsStoreFn,
        BiFunction<CypherGraphStore, TokenHolders, InMemoryTransactionStateVisitor> txStateVisitorFn,
        MetadataProvider metadataProvider,
        Supplier<CommandCreationContext> commandCreationContextSupplier,
        TriFunction<CypherGraphStore, TokenHolders, CountsStore, StorageReader> storageReaderFn
    ) {
        super(
            databaseLayout,
            tokenHolders,
            countsStoreFn,
            txStateVisitorFn,
            metadataProvider,
            commandCreationContextSupplier,
            storageReaderFn
        );
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
        ReadableTransactionState state,
        StorageReader storageReader,
        CommandCreationContext commandCreationContext,
        ResourceLocker resourceLocker,
        LockTracer lockTracer,
        TxStateVisitor.Decorator decorator,
        CursorContext cursorContext,
        StoreCursors storeCursors,
        MemoryTracker memoryTracker
    ) throws KernelException {
        super.createCommands(state);
    }

    @Override
    public void dumpDiagnostics(InternalLog internalLog, DiagnosticsLogger diagnosticsLogger) {
    }

    @Override
    public List<StorageCommand> createUpgradeCommands(KernelVersion kernelVersion) {
        return null;
    }

    @Override
    public StoreId retrieveStoreId() {
        return metadataProvider.getStoreId();
    }

    @Override
    public StorageEngineIndexingBehaviour indexingBehaviour() {
        return null;
    }

    public static final class Builder extends InMemoryStorageEngineBuilder<InMemoryStorageEngineImpl> {
        public Builder(
            DatabaseLayout databaseLayout,
            TokenHolders tokenHolders,
            MetadataProvider metadataProvider
        ) {
            super(databaseLayout, tokenHolders, metadataProvider);
        }

        @Override
        public InMemoryStorageEngineImpl build() {
            return new InMemoryStorageEngineImpl(
                databaseLayout,
                tokenHolders,
                countsStoreFn,
                txStateVisitorFn,
                metadataProvider,
                commandCreationContextSupplier,
                storageReaderFn
            );
        }
    }

    @Override
    public void rollback(ReadableTransactionState txState, CursorContext cursorContext) {
        // rollback is not supported but it is also called when we fail for something else
        // that we do not support, such as removing node properties
        // TODO: do we want to inspect the txState to infer if rollback was called explicitly or not?
    }

    @Override
    public void checkpoint(DatabaseFlushEvent flushEvent, CursorContext cursorContext) {
        // checkpoint is not supported but it is also called when we fail for something else
        // that we do not support, such as removing node properties
    }
}
