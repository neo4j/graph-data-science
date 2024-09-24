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

import org.neo4j.gds.compat.PartitionedStoreScan;
import org.neo4j.gds.compat.StoreScan;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.kernel.api.KernelTransaction;

final class NodeCursorBasedScanner extends AbstractNodeCursorBasedScanner<NodeCursor> {

    private final long nodeCount;

    NodeCursorBasedScanner(int prefetchSize, long nodeCount, TransactionContext transaction) {
        super(prefetchSize, transaction);
        this.nodeCount = nodeCount;
    }

    @Override
    NodeCursor entityCursor(KernelTransaction transaction) {
        return transaction.cursors().allocateNodeCursor(transaction.cursorContext());
    }

    @Override
    StoreScan<NodeCursor> entityCursorScan(KernelTransaction transaction) {
        int batchSize = batchSize();
        int numberOfPartitions = PartitionedStoreScan.getNumberOfPartitions(this.nodeCount, batchSize);
        return new PartitionedStoreScan<>(transaction.dataRead()
            .allNodesScan(numberOfPartitions, transaction.cursorContext()));
    }

    @Override
    NodeReference cursorReference(KernelTransaction transaction, NodeCursor cursor) {
        return new NodeCursorReference(cursor);
    }

    @Override
    void closeCursorReference(NodeReference nodeReference) {
        // no need to close anything, nothing new is allocated in `cursorReference`
    }
}
