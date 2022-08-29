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
package org.neo4j.gds.core.loading;

import org.neo4j.gds.compat.StoreScan;
import org.neo4j.gds.mem.BitUtil;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.kernel.api.KernelTransaction;

abstract class AbstractCursorBasedScanner<Reference, EntityCursor extends Cursor>
    implements StoreScanner<Reference> {

    private final class ScanCursor implements StoreScanner.ScanCursor<Reference> {

        private EntityCursor cursor;
        private Reference cursorReference;
        private final StoreScan<EntityCursor> scan;
        private final KernelTransaction ktx;

        ScanCursor(
            EntityCursor cursor,
            Reference reference,
            StoreScan<EntityCursor> entityCursorScan,
            KernelTransaction ktx
        ) {
            this.cursor = cursor;
            this.cursorReference = reference;
            this.scan = entityCursorScan;
            this.ktx = ktx;
        }

        @Override
        public boolean reserveBatch() {
            return scan.reserveBatch(cursor, ktx);
        }

        @Override
        public boolean consumeBatch(RecordConsumer<Reference> consumer) {
            while (cursor.next()) {
                if (!consumer.offer(cursorReference)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public void close() {
            if (cursor != null) {
                closeCursorReference(cursorReference);
                cursorReference = null;
                cursor.close();
                cursor = null;
            }
        }
    }

    // fetch this many pages at once
    private final int prefetchSize;

    private final TransactionContext.SecureTransaction transaction;

    private volatile StoreScan<EntityCursor> entityCursorScan;

    AbstractCursorBasedScanner(int prefetchSize, TransactionContext transactionContext) {
        this.transaction = transactionContext.fork();
        this.prefetchSize = prefetchSize;
    }

    @Override
    public void close() {
        transaction.close();
    }

    @Override
    public final StoreScanner.ScanCursor<Reference> createCursor(KernelTransaction transaction) {
        var entityCursorScan = createEntityCursorScan();
        EntityCursor entityCursor = entityCursor(transaction);
        Reference reference = cursorReference(transaction, entityCursor);
        return new ScanCursor(
            entityCursor,
            reference,
            entityCursorScan,
            transaction
        );
    }

    private StoreScan<EntityCursor> createEntityCursorScan() {
        var scan = this.entityCursorScan;
        if (scan != null) {
            return scan;
        }
        synchronized (this) {
            scan = this.entityCursorScan;
            if (scan != null) {
                return scan;
            }

            scan = entityCursorScan(this.transaction.kernelTransaction());
            this.entityCursorScan = scan;
            return scan;
        }
    }

    abstract int recordsPerPage();

    abstract EntityCursor entityCursor(KernelTransaction transaction);

    abstract StoreScan<EntityCursor> entityCursorScan(KernelTransaction transaction);

    abstract Reference cursorReference(KernelTransaction transaction, EntityCursor cursor);

    /**
     * Close Neo4j cursors that have been allocated in {@link #cursorReference(org.neo4j.kernel.api.KernelTransaction, org.neo4j.internal.kernel.api.Cursor)}.
     * Cursors that were allocated in {@link #entityCursor(org.neo4j.kernel.api.KernelTransaction)} are closed automatically.
     */
    abstract void closeCursorReference(Reference reference);

    int batchSize() {
        // We want to scan about 100 pages per bulk, so start with that value
        var bulkSize = prefetchSize * recordsPerPage();

        // When initializing cursors using a scan, kernel might align the given
        // batch size by 64. Since we use the batch size to allocate our buffers,
        // we always align.
        bulkSize = (int) BitUtil.align(bulkSize, 64);

        return bulkSize;
    }

    @Override
    public int bufferSize() {
        return batchSize();
    }
}
