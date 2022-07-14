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

import org.neo4j.gds.compat.CompositeNodeCursor;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.StoreScan;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

final class MultipleNodeLabelIndexBasedScanner extends AbstractNodeCursorBasedScanner<CompositeNodeCursor, int[]> {

    private final int[] labelIds;
    private final boolean allowPartitionedScan;

    MultipleNodeLabelIndexBasedScanner(
        int[] labelIds,
        int prefetchSize,
        TransactionContext transaction,
        boolean allowPartitionedScan
    ) {
        super(prefetchSize, transaction, labelIds);
        this.labelIds = labelIds;
        this.allowPartitionedScan = allowPartitionedScan;
    }

    @Override
    CompositeNodeCursor entityCursor(KernelTransaction transaction) {
        List<NodeLabelIndexCursor> cursors = Arrays
            .stream(labelIds)
            .mapToObj(i -> Neo4jProxy.allocateNodeLabelIndexCursor(transaction))
            .collect(Collectors.toList());
        return Neo4jProxy.compositeNodeCursor(cursors, labelIds);
    }

    @Override
    StoreScan<CompositeNodeCursor> entityCursorScan(KernelTransaction transaction, int[] labelIds) {
        return new CompositeNodeScan(Neo4jProxy.entityCursorScan(
            transaction,
            labelIds,
            batchSize(),
            allowPartitionedScan
        ));
    }

    @Override
    NodeReference cursorReference(KernelTransaction transaction, CompositeNodeCursor cursor) {
        return new MultipleNodeLabelIndexReference(
            cursor,
            transaction.dataRead(),
            Neo4jProxy.allocateNodeCursor(transaction)
        );
    }

    @Override
    void closeCursorReference(NodeReference nodeReference) {
        ((MultipleNodeLabelIndexReference) nodeReference).close();
    }
}
