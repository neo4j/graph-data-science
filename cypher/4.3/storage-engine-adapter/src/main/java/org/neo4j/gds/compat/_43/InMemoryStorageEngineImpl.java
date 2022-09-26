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
package org.neo4j.gds.compat._43;

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
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.InjectedNLIUpgradeCallback;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.Log;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StoreId;
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
    ) throws KernelException {
        super.createCommands(txState);
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
}
