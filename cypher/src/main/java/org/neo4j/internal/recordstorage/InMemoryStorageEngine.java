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

import org.neo4j.counts.CountsAccessor;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.api.InjectedNLIUpgradeCallback;
import org.neo4j.kernel.lifecycle.Lifecycle;
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

import java.util.Collection;
import java.util.List;

public class InMemoryStorageEngine implements StorageEngine, Lifecycle {

    public InMemoryStorageEngine() {
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
        throw new UnsupportedOperationException();
    }

    @Override
    public Lifecycle schemaAndTokensLifecycle() {
        throw new UnsupportedOperationException();
    }

    @Override
    public CountsAccessor countsAccessor() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dumpDiagnostics(Log errorLog, DiagnosticsLogger diagnosticsLog) {

    }

    @Override
    public MetadataProvider metadataProvider() {
        throw new UnsupportedOperationException();
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
