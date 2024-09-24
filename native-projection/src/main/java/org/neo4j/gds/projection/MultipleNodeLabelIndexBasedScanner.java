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

import org.neo4j.gds.compat.CompositeNodeCursor;
import org.neo4j.gds.compat.PartitionedStoreScan;
import org.neo4j.gds.compat.StoreScan;
import org.neo4j.gds.transaction.TransactionContext;
import org.neo4j.internal.kernel.api.NodeLabelIndexCursor;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

final class MultipleNodeLabelIndexBasedScanner extends AbstractNodeCursorBasedScanner<CompositeNodeCursor> {

    private final int[] labelIds;

    MultipleNodeLabelIndexBasedScanner(
        int[] labelIds,
        int prefetchSize,
        TransactionContext transaction
    ) {
        super(prefetchSize, transaction);
        this.labelIds = labelIds;
    }

    @Override
    CompositeNodeCursor entityCursor(KernelTransaction transaction) {
        List<NodeLabelIndexCursor> cursors = Arrays
            .stream(labelIds)
            .mapToObj(i -> transaction.cursors().allocateNodeLabelIndexCursor(transaction.cursorContext()))
            .collect(Collectors.toList());
        return new CompositeNodeCursor(cursors, labelIds);
    }

    @Override
    StoreScan<CompositeNodeCursor> entityCursorScan(KernelTransaction transaction) {
        return new CompositeNodeScan(PartitionedStoreScan.createScans(
            transaction,
            batchSize(),
            labelIds
        ));
    }

    @Override
    NodeReference cursorReference(KernelTransaction transaction, CompositeNodeCursor cursor) {
        return new MultipleNodeLabelIndexReference(
            cursor,
            transaction.dataRead(),
            transaction.cursors().allocateNodeCursor(transaction.cursorContext())
        );
    }

    @Override
    void closeCursorReference(NodeReference nodeReference) {
        ((MultipleNodeLabelIndexReference) nodeReference).close();
    }
}
