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

import org.neo4j.internal.recordstorage.AbstractInMemoryMetaDataProvider;
import org.neo4j.internal.recordstorage.AbstractTransactionIdStore;
import org.neo4j.internal.recordstorage.InMemoryLogVersionRepository43;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.ExternalStoreId;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;

import java.util.Optional;

public class InMemoryMetaDataProviderImpl extends AbstractInMemoryMetaDataProvider {
    private final InMemoryTransactionIdStoreImpl transactionIdStore;

    InMemoryMetaDataProviderImpl() {
        super(new InMemoryLogVersionRepository43());
        this.transactionIdStore = new InMemoryTransactionIdStoreImpl();
    }

    @Override
    public Optional<ExternalStoreId> getExternalStoreId() {
        return Optional.of(this.externalStoreId);
    }

    @Override
    public long[] getLastClosedTransaction() {
        return this.transactionIdStore.getLastClosedTransaction();
    }

    @Override
    public AbstractTransactionIdStore transactionIdStore() {
        return transactionIdStore;
    }

    @Override
    public void transactionClosed(
        long transactionId, long logVersion, long byteOffset, CursorContext cursorContext
    ) {
        this.transactionIdStore().transactionClosed(transactionId, logVersion, byteOffset, cursorContext);
    }

    @Override
    public void resetLastClosedTransaction(
        long transactionId, long logVersion, long byteOffset, boolean missingLogs, CursorContext cursorContext
    ) {
        this.transactionIdStore().resetLastClosedTransaction(
            transactionId,
            logVersion,
            byteOffset,
            missingLogs,
            cursorContext
        );
    }

    @Override
    public void setCurrentLogVersion(long version, CursorContext cursorContext) {
        this.logVersionRepository.setCurrentLogVersion(version, cursorContext);
    }

    @Override
    public long incrementAndGetVersion(CursorContext cursorContext) {
        return this.logVersionRepository.incrementAndGetVersion(cursorContext);
    }

    @Override
    public void setCheckpointLogVersion(long version, CursorContext cursorContext) {
        this.logVersionRepository.setCheckpointLogVersion(version, cursorContext);
    }

    @Override
    public long incrementAndGetCheckpointLogVersion(CursorContext cursorContext) {
        return this.logVersionRepository.incrementAndGetCheckpointLogVersion(cursorContext);
    }

    @Override
    public void transactionCommitted(long transactionId, int checksum, long commitTimestamp, CursorContext cursorContext) {
        this.transactionIdStore().transactionCommitted(transactionId, checksum, commitTimestamp, cursorContext);
    }

    @Override
    public TransactionId getUpgradeTransaction() {
        return this.transactionIdStore().getUpgradeTransaction();
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
        long transactionId,
        int checksum,
        long commitTimestamp,
        long byteOffset,
        long logVersion,
        CursorContext cursorContext
    ) {
        this.transactionIdStore().setLastCommittedAndClosedTransactionId(
            transactionId,
            checksum,
            commitTimestamp,
            byteOffset,
            logVersion,
            cursorContext
        );
    }

    @Override
    public void flush(CursorContext cursorContext) {
        this.transactionIdStore().flush(cursorContext);
    }

    @Override
    public StoreId getStoreId() {
        return StoreId.UNKNOWN;
    }
}
