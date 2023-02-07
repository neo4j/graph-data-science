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
package org.neo4j.gds.compat._55;

import org.neo4j.internal.recordstorage.AbstractTransactionIdStore;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.storageengine.api.ClosedTransactionMetadata;

public class InMemoryTransactionIdStoreImpl extends AbstractTransactionIdStore {

    public ClosedTransactionMetadata getLastClosedTransaction() {
        long[] metaData = this.closedTransactionId.get();
        return new ClosedTransactionMetadata(
            metaData[0],
            new LogPosition(metaData[1], metaData[2]),
            (int) metaData[3],
            metaData[4]
        );
    }

    @Override
    public void transactionClosed(
        long transactionId,
        long logVersion,
        long byteOffset,
        int checksum,
        long commitTimestamp
    ) {
        this.closedTransactionId.offer(transactionId, new long[]{logVersion, byteOffset, checksum, commitTimestamp});
    }

    @Override
    public void resetLastClosedTransaction(
        long transactionId,
        long logVersion,
        long byteOffset,
        int checksum,
        long commitTimestamp
    ) {
        this.closedTransactionId.set(transactionId, new long[]{logVersion, byteOffset, checksum, commitTimestamp});
    }

    @Override
    public void transactionCommitted(long transactionId, int checksum, long commitTimestamp) {
    }

    @Override
    public void setLastCommittedAndClosedTransactionId(
        long transactionId, int checksum, long commitTimestamp, long byteOffset, long logVersion
    ) {
    }

    @Override
    protected CursorContext getEmptyCursorContext() {
        return CursorContext.NULL_CONTEXT;
    }
}
