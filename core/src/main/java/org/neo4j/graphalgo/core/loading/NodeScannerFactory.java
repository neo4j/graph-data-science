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
package org.neo4j.graphalgo.core.loading;

import org.neo4j.common.EntityType;
import org.neo4j.graphalgo.core.SecureTransaction;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.kernel.api.KernelTransaction;

import java.util.Arrays;
import java.util.Iterator;

import static org.neo4j.graphalgo.core.GraphDimensions.ANY_LABEL;

public final class NodeScannerFactory {

    private NodeScannerFactory() {}

    public static StoreScanner.Factory<NodeReference> create(SecureTransaction secureTransaction, int[] labelIds) {
        if (Arrays.stream(labelIds).anyMatch(labelId -> labelId == ANY_LABEL) || !hasNodeLabelIndex(secureTransaction)) {
            return NodeCursorBasedScanner::new;
        } else if (labelIds.length == 1) {
            return (prefetchSize, transaction) -> new NodeLabelIndexBasedScanner(labelIds[0], prefetchSize, transaction);
        } else {
            return (prefetchSize, transaction) -> new MultipleNodeLabelIndexBasedScanner(labelIds, prefetchSize, transaction);
        }
    }

    private static boolean hasNodeLabelIndex(SecureTransaction secureTransaction) {
        try (var forkedTransaction = secureTransaction.fork()) {
            return findUsableMatchingIndex(
                // .get() is safe since we fork the transaction
                forkedTransaction.topLevelKernelTransaction().get(),
                SchemaDescriptor.forAnyEntityTokens(EntityType.NODE)
            ) != IndexDescriptor.NO_INDEX;
        }
    }

    private static IndexDescriptor findUsableMatchingIndex(
        KernelTransaction transaction,
        SchemaDescriptor schemaDescriptor
    ) {
        SchemaRead schemaRead = transaction.schemaRead();
        Iterator<IndexDescriptor> iterator = schemaRead.index(schemaDescriptor);
        while (iterator.hasNext()) {
            IndexDescriptor index = iterator.next();
            if (index.getIndexType() != IndexType.FULLTEXT && indexIsOnline(schemaRead, index)) {
                return index;
            }
        }
        return IndexDescriptor.NO_INDEX;
    }

    private static boolean indexIsOnline(SchemaRead schemaRead, IndexDescriptor index) {
        InternalIndexState state = InternalIndexState.FAILED;
        try {
            state = schemaRead.indexGetState(index);
        } catch (IndexNotFoundKernelException e) {
            // Well the index should always exist here, but if we didn't find it while checking the state,
            // then we obviously don't want to use it.
        }
        return state == InternalIndexState.ONLINE;
    }
}
