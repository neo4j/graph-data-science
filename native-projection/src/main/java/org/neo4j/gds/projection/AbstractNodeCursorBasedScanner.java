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
package org.neo4j.gds.projection;

import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.kernel.api.Cursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;

abstract class AbstractNodeCursorBasedScanner<EntityCursor extends Cursor>
    extends AbstractCursorBasedScanner<NodeReference, EntityCursor> {

    AbstractNodeCursorBasedScanner(int prefetchSize, TransactionContext transaction) {
        super(prefetchSize, transaction);
    }

    @Override
    int recordsPerPage() {
        return PageCache.PAGE_SIZE / NodeRecordFormat.RECORD_SIZE;
    }

    @Override
    public final long storeSize(GraphDimensions graphDimensions) {
        long recordsInUse = graphDimensions.highestPossibleNodeCount();
        long idsInPages = ((recordsInUse + (recordsPerPage() - 1L)) / recordsPerPage()) * recordsPerPage();
        return idsInPages * (long) NodeRecordFormat.RECORD_SIZE;
    }
}
