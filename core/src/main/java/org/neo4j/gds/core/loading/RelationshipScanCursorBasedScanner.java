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

import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.StoreScan;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.format.standard.RelationshipRecordFormat;

public final class RelationshipScanCursorBasedScanner extends AbstractCursorBasedScanner<RelationshipReference, RelationshipScanCursor> {

    public static final StoreScanner.Factory<RelationshipReference> FACTORY = RelationshipScanCursorBasedScanner::new;

    private RelationshipScanCursorBasedScanner(int prefetchSize, TransactionContext transaction) {
        super(prefetchSize, transaction);
    }

    @Override
    int recordsPerPage() {
        return PageCache.PAGE_SIZE / RelationshipRecordFormat.RECORD_SIZE;
    }

    @Override
    public long storeSize(GraphDimensions graphDimensions) {
        long recordsInUse = 1L + graphDimensions.highestRelationshipId();
        long idsInPages = ((recordsInUse + (recordsPerPage() - 1L)) / recordsPerPage()) * recordsPerPage();
        return idsInPages * (long) RelationshipRecordFormat.RECORD_SIZE;
    }

    @Override
    RelationshipScanCursor entityCursor(KernelTransaction transaction) {
        return Neo4jProxy.allocateRelationshipScanCursor(transaction);
    }

    @Override
    StoreScan<RelationshipScanCursor> entityCursorScan(KernelTransaction transaction) {
        return Neo4jProxy.scanToStoreScan(transaction.dataRead().allRelationshipsScan(), batchSize());
    }

    @Override
    RelationshipReference cursorReference(KernelTransaction transaction, RelationshipScanCursor cursor) {
        return new RelationshipScanCursorReference(cursor);
    }

    @Override
    void closeCursorReference(RelationshipReference relationshipReference) {
        // no need to close anything, nothing new is allocated in `cursorReference`
    }
}
