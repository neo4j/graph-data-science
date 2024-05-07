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
package org.neo4j.gds.compat._519;

import org.neo4j.internal.recordstorage.AbstractTransactionIdStore;
import org.neo4j.io.pagecache.context.TransactionIdSnapshot;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

public class InMemoryTransactionIdStoreImpl extends AbstractTransactionIdStore {

    @Override
    protected OutOfOrderSequence createClosedTransactionId() {
        return new ArrayQueueOutOfOrderSequence(
            -1L,
            100,
            new OutOfOrderSequence.Meta(0, 0, 0, 0, 0)
        );
    }

    @Override
    protected void initLastCommittedAndClosedTransactionId(
        long previouslyCommittedTxId,
        int checksum,
        long previouslyCommittedTxCommitTimestamp,
        long previouslyCommittedTxLogByteOffset,
        long previouslyCommittedTxLogVersion
    ) {
        this.setLastCommittedAndClosedTransactionId(
            previouslyCommittedTxId,
            checksum,
            previouslyCommittedTxCommitTimestamp,
            TransactionIdStore.UNKNOWN_CONSENSUS_INDEX,
            previouslyCommittedTxLogByteOffset,
            previouslyCommittedTxLogVersion
        );
    }

    @Override
    public ClosedTransactionMetadata getLastClosedTransaction() {
        var numberWithMeta = this.closedTransactionId.get();
        OutOfOrderSequence.Meta meta = numberWithMeta.meta();
        return new ClosedTransactionMetadata(
            numberWithMeta.number(),
            new LogPosition(meta.logVersion(), meta.byteOffset()),
            meta.checksum(),
            meta.commitTimestamp(),
            meta.consensusIndex()
        );
    }

    @Override
    public TransactionIdSnapshot getClosedTransactionSnapshot() {
        return new TransactionIdSnapshot(this.getLastClosedTransactionId());
    }

    @Override
    protected TransactionId transactionId(long transactionId, int checksum, long commitTimestamp) {
        return new TransactionId(transactionId, checksum, commitTimestamp, TransactionIdStore.UNKNOWN_CONSENSUS_INDEX);
    }

    @Override
    public void transactionCommitted(long transactionId, int checksum, long commitTimestamp, long consensusIndex) {

    }

    @Override
    public long getLastCommittedTransactionId() {
        return this.committedTransactionId.get().transactionId();
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
        long transactionId,
        int checksum,
        long commitTimestamp,
        long consensusIndex,
        long byteOffset,
        long logVersion
    ) {

    }

    @Override
    public void transactionClosed(
        long transactionId,
        long logVersion,
        long byteOffset,
        int checksum,
        long commitTimestamp,
        long consensusIndex
    ) {
        this.closedTransactionId.offer(
            transactionId,
            new OutOfOrderSequence.Meta(logVersion, byteOffset, checksum, commitTimestamp, consensusIndex)
        );
    }

    @Override
    public void resetLastClosedTransaction(
        long transactionId,
        long logVersion,
        long byteOffset,
        int checksum,
        long commitTimestamp,
        long consensusIndex
    ) {
        this.closedTransactionId.set(
            transactionId,
            new OutOfOrderSequence.Meta(logVersion, byteOffset, checksum, commitTimestamp, consensusIndex)
        );
    }
}
