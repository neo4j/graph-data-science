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
package org.neo4j.gds.compat._54;

import org.neo4j.internal.recordstorage.InMemoryLogVersionRepository;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

public class InMemoryMetaDataProviderImpl implements MetadataProvider {

    private final ExternalStoreId externalStoreId;
    private final InMemoryLogVersionRepository logVersionRepository;
    private final InMemoryTransactionIdStoreImpl transactionIdStore;

    InMemoryMetaDataProviderImpl() {
        this.logVersionRepository = new InMemoryLogVersionRepository();
        this.externalStoreId = new ExternalStoreId(UUID.randomUUID());
        this.transactionIdStore = new InMemoryTransactionIdStoreImpl();
    }

    @Override
    public ExternalStoreId getExternalStoreId() {
        return this.externalStoreId;
    }

    @Override
    public ClosedTransactionMetadata getLastClosedTransaction() {
        return this.transactionIdStore.getLastClosedTransaction();
    }

    @Override
    public void transactionClosed(
        long transactionId,
        long logVersion,
        long byteOffset,
        int checksum,
        long commitTimestamp
    ) {
        this.transactionIdStore.transactionClosed(
            transactionId,
            logVersion,
            byteOffset,
            checksum,
            commitTimestamp
        );
    }

    @Override
    public void resetLastClosedTransaction(
        long transactionId,
        long logVersion,
        long byteOffset,
        int checksum,
        long commitTimestamp
    ) {
        this.transactionIdStore.resetLastClosedTransaction(
            transactionId,
            logVersion,
            byteOffset,
            checksum,
            commitTimestamp
        );
    }

    @Override
    public void setCurrentLogVersion(long version) {
        logVersionRepository.setCurrentLogVersion(version);
    }

    @Override
    public long incrementAndGetVersion() {
        return logVersionRepository.incrementAndGetVersion();
    }

    @Override
    public void setCheckpointLogVersion(long version) {
        logVersionRepository.setCheckpointLogVersion(version);
    }

    @Override
    public long incrementAndGetCheckpointLogVersion() {
        return logVersionRepository.incrementAndGetCheckpointLogVersion();
    }

    @Override
    public void transactionCommitted(long transactionId, int checksum, long commitTimestamp) {
        transactionIdStore.transactionCommitted(transactionId, checksum, commitTimestamp);
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
        long transactionId, int checksum, long commitTimestamp, long byteOffset, long logVersion
    ) {
        transactionIdStore.setLastCommittedAndClosedTransactionId(
            transactionId,
            checksum,
            commitTimestamp,
            byteOffset,
            logVersion
        );
    }

    @Override
    public void regenerateMetadata(StoreId storeId, UUID externalStoreUUID, CursorContext cursorContext) {
    }

    @Override
    public StoreId getStoreId() {
        return StoreId.UNKNOWN;
    }

    @Override
    public void setKernelVersion(KernelVersion kernelVersion) {
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public long getCurrentLogVersion() {
        return this.logVersionRepository.getCurrentLogVersion();
    }

    @Override
    public long getCheckpointLogVersion() {
        return this.logVersionRepository.getCheckpointLogVersion();
    }

    @Override
    public long nextCommittingTransactionId() {
        return this.transactionIdStore.nextCommittingTransactionId();
    }

    @Override
    public long committingTransactionId() {
        return this.transactionIdStore.committingTransactionId();
    }

    @Override
    public long getLastCommittedTransactionId() {
        return this.transactionIdStore.getLastCommittedTransactionId();
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return this.transactionIdStore.getLastCommittedTransaction();
    }

    @Override
    public long getLastClosedTransactionId() {
        return this.transactionIdStore.getLastClosedTransactionId();
    }

    @Override
    public KernelVersion kernelVersion() {
        return KernelVersion.LATEST;
    }

    @Override
    public Optional<UUID> getDatabaseIdUuid(CursorContext cursorTracer) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void setDatabaseIdUuid(UUID uuid, CursorContext cursorContext) {
        throw new IllegalStateException("Not supported");
    }

    TransactionIdStore transactionIdStore() {
        return this.transactionIdStore;
    }
}
