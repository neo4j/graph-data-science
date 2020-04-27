/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Scan;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;

public class RelationshipScanCursorScanner extends AbstractCursorScanner<RelationshipReference, RelationshipScanCursor, RelationshipStore> {

    public static final StoreScanner.Factory<RelationshipReference> FACTORY = RelationshipScanCursorScanner::new;

    RelationshipScanCursorScanner(int prefetchSize, GraphDatabaseService api) {
        super(prefetchSize, api);
    }

    @Override
    RelationshipStore store(NeoStores neoStores) {
        return neoStores.getRelationshipStore();
    }

    @Override
    RelationshipScanCursor entityCursor(KernelTransaction transaction) {
        return transaction.cursors().allocateRelationshipScanCursor(PageCursorTracer.NULL);
    }

    @Override
    Scan<RelationshipScanCursor> entityCursorScan(KernelTransaction transaction) {
        return transaction.dataRead().allRelationshipsScan();
    }

    @Override
    RelationshipReference cursorReference(RelationshipScanCursor cursor) {
        return new RelationshipScanCursorReference(cursor);
    }
}
