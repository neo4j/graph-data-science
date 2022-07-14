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
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.kernel.api.KernelTransaction;

final class NodeLabelIndexBasedScanner extends AbstractNodeCursorBasedScanner<NodeLabelIndexCursor, Integer> {

    private final int labelId;
    private final boolean allowPartitionedScan;

    NodeLabelIndexBasedScanner(
        int labelId,
        int prefetchSize,
        TransactionContext transaction,
        boolean allowPartitionedScan
    ) {
        super(prefetchSize, transaction, labelId);
        this.labelId = labelId;
        this.allowPartitionedScan = allowPartitionedScan;
    }

    @Override
    NodeLabelIndexCursor entityCursor(KernelTransaction transaction) {
        return Neo4jProxy.allocateNodeLabelIndexCursor(transaction);
    }

    @Override
    StoreScan<NodeLabelIndexCursor> entityCursorScan(KernelTransaction transaction, Integer labelId) {
        return Neo4jProxy.nodeLabelIndexScan(
            transaction,
            labelId,
            batchSize(),
            allowPartitionedScan
        );
    }

    @Override
    NodeReference cursorReference(KernelTransaction transaction, NodeLabelIndexCursor cursor) {
        return new NodeLabelIndexReference(
            cursor,
            transaction.dataRead(),
            Neo4jProxy.allocateNodeCursor(transaction),
            labelId
        );
    }

    @Override
    void closeCursorReference(NodeReference nodeReference) {
        ((NodeLabelIndexReference) nodeReference).close();
    }
}
