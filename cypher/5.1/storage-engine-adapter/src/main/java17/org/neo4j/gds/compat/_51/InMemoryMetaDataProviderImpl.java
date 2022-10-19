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

import org.neo4j.internal.recordstorage.AbstractInMemoryMetaDataProvider;
import org.neo4j.internal.recordstorage.AbstractTransactionIdStore;
import org.neo4j.internal.recordstorage.InMemoryLogVersionRepository51;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.StoreId;

import java.util.UUID;

public class InMemoryMetaDataProviderImpl extends AbstractInMemoryMetaDataProvider {

    private final InMemoryTransactionIdStoreImpl transactionIdStore;

    InMemoryMetaDataProviderImpl() {
        super(new InMemoryLogVersionRepository51());
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
    public AbstractTransactionIdStore transactionIdStore() {
        return transactionIdStore;
    }

    @Override
    public void regenerateMetadata(StoreId storeId, UUID externalStoreUUID, CursorContext cursorContext) {
    }

    @Override
    public StoreId getStoreId() {
        return StoreId.UNKNOWN;
    }
}
