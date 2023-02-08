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

import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.util.concurrent.ArrayQueueOutOfOrderSequence;
import org.neo4j.util.concurrent.OutOfOrderSequence;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractTransactionIdStore implements TransactionIdStore {
    protected final AtomicLong committingTransactionId;
    protected final OutOfOrderSequence closedTransactionId;
    protected final AtomicReference<TransactionId> committedTransactionId;
    protected final long previouslyCommittedTxId;
    protected final int initialTransactionChecksum;
    protected final long previouslyCommittedTxCommitTimestamp;

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
        this.closedTransactionId = new ArrayQueueOutOfOrderSequence(-1L, 100, new long[5]);
        this.committedTransactionId = new AtomicReference<>(transactionId(1L, -559063315, 0L));

        assert previouslyCommittedTxId >= 1L : "cannot start from a tx id less than BASE_TX_ID";

        initLastCommittedAndClosedTransactionId(
            previouslyCommittedTxId,
            checksum,
            previouslyCommittedTxCommitTimestamp,
            previouslyCommittedTxLogVersion,
            previouslyCommittedTxLogByteOffset
        );
        this.previouslyCommittedTxId = previouslyCommittedTxId;
        this.initialTransactionChecksum = checksum;
        this.previouslyCommittedTxCommitTimestamp = previouslyCommittedTxCommitTimestamp;
    }

    protected abstract void initLastCommittedAndClosedTransactionId(
        long previouslyCommittedTxId,
        int checksum,
        long previouslyCommittedTxCommitTimestamp,
        long previouslyCommittedTxLogByteOffset,
        long previouslyCommittedTxLogVersion
    );

    protected abstract TransactionId transactionId(long transactionId, int checksum, long commitTimestamp);

    @Override
    public long nextCommittingTransactionId() {
        return this.committingTransactionId.incrementAndGet();
    }

    @Override
    public long committingTransactionId() {
        return this.committingTransactionId.get();
    }

    @Override
    public long getLastCommittedTransactionId() {
        return this.committedTransactionId.get().transactionId();
    }

    @Override
    public TransactionId getLastCommittedTransaction() {
        return this.committedTransactionId.get();
    }

    @Override
    public long getLastClosedTransactionId() {
        return this.closedTransactionId.getHighestGapFreeNumber();
    }
}
