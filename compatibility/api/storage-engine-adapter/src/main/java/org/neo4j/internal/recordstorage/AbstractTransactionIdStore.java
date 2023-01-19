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

import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractTransactionIdStore implements TransactionIdStore {
    private final AtomicLong committingTransactionId;
    protected final OutOfOrderSequence closedTransactionId;
    private final AtomicReference<TransactionId> committedTransactionId;
    private final long previouslyCommittedTxId;
    private final int initialTransactionChecksum;
    private final long previouslyCommittedTxCommitTimestamp;

    public AbstractTransactionIdStore() {
        this(1L, -559063315, 0L, 0L, 64L);
    }

    public AbstractTransactionIdStore(
        long previouslyCommittedTxId,
        int checksum,
        long previouslyCommittedTxCommitTimestamp,
        long previouslyCommittedTxLogVersion,
        long previouslyCommittedTxLogByteOffset
    ) {
        this.committingTransactionId = new AtomicLong();
        this.closedTransactionId = new ArrayQueueOutOfOrderSequence(-1L, 100, new long[1]);
        this.committedTransactionId = new AtomicReference<>(new TransactionId(1L, -559063315, 0L));

        assert previouslyCommittedTxId >= 1L : "cannot start from a tx id less than BASE_TX_ID";

        this.setLastCommittedAndClosedTransactionId(
            previouslyCommittedTxId,
            checksum,
            previouslyCommittedTxCommitTimestamp,
            previouslyCommittedTxLogByteOffset,
            previouslyCommittedTxLogVersion,
            getEmptyCursorContext()
        );
        this.previouslyCommittedTxId = previouslyCommittedTxId;
        this.initialTransactionChecksum = checksum;
        this.previouslyCommittedTxCommitTimestamp = previouslyCommittedTxCommitTimestamp;
    }

    protected abstract CursorContext getEmptyCursorContext();

    public long nextCommittingTransactionId() {
        return this.committingTransactionId.incrementAndGet();
    }

    public long committingTransactionId() {
        return this.committingTransactionId.get();
    }

    public synchronized void transactionCommitted(
        long transactionId,
        int checksum,
        long commitTimestamp,
        CursorContext cursorContext
    ) {
        TransactionId current = this.committedTransactionId.get();
        if (current == null || transactionId > current.transactionId()) {
            this.committedTransactionId.set(new TransactionId(transactionId, checksum, commitTimestamp));
        }

    }

    public long getLastCommittedTransactionId() {
        return this.committedTransactionId.get().transactionId();
    }

    public TransactionId getLastCommittedTransaction() {
        return this.committedTransactionId.get();
    }

    public TransactionId getUpgradeTransaction() {
        return new TransactionId(
            this.previouslyCommittedTxId,
            this.initialTransactionChecksum,
            this.previouslyCommittedTxCommitTimestamp
        );
    }

    public long getLastClosedTransactionId() {
        return this.closedTransactionId.getHighestGapFreeNumber();
    }

    public void setLastCommittedAndClosedTransactionId(
        long transactionId,
        int checksum,
        long commitTimestamp,
        long byteOffset,
        long logVersion,
        CursorContext cursorContext
    ) {
        this.committingTransactionId.set(transactionId);
        this.committedTransactionId.set(new TransactionId(transactionId, checksum, commitTimestamp));
        this.closedTransactionId.set(transactionId, new long[]{logVersion, byteOffset, checksum, commitTimestamp});
    }

    public void transactionClosed(long transactionId, long logVersion, long byteOffset, CursorContext cursorContext) {
        this.closedTransactionId.offer(transactionId, new long[]{logVersion, byteOffset});
    }

    public void resetLastClosedTransaction(
        long transactionId,
        long byteOffset,
        long logVersion,
        boolean missingLogs,
        CursorContext cursorContext
    ) {
        this.closedTransactionId.set(transactionId, new long[]{logVersion, byteOffset});
    }

    public void flush(CursorContext cursorContext) {
    }
}
