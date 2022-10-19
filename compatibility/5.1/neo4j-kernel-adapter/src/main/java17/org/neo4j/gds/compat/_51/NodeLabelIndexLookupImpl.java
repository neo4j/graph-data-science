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
package org.neo4j.gds.compat._51;

import org.neo4j.common.EntityType;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.internal.schema.SchemaDescriptors;
import org.neo4j.kernel.api.KernelTransaction;

final class NodeLabelIndexLookupImpl {

    static boolean hasNodeLabelIndex(KernelTransaction transaction) {
        return NodeLabelIndexLookupImpl.findUsableMatchingIndex(
            transaction,
            SchemaDescriptors.forAnyEntityTokens(EntityType.NODE)
        ) != IndexDescriptor.NO_INDEX;
    }

    static IndexDescriptor findUsableMatchingIndex(
        KernelTransaction transaction,
        SchemaDescriptor schemaDescriptor
    ) {
        var schemaRead = transaction.schemaRead();
        var iterator = schemaRead.index(schemaDescriptor);
        while (iterator.hasNext()) {
            var index = iterator.next();
            if (index.getIndexType() == IndexType.LOOKUP && indexIsOnline(schemaRead, index)) {
                return index;
            }
        }
        return IndexDescriptor.NO_INDEX;
    }

    private static boolean indexIsOnline(SchemaRead schemaRead, IndexDescriptor index) {
        var state = InternalIndexState.FAILED;
        try {
            state = schemaRead.indexGetState(index);
        } catch (IndexNotFoundKernelException e) {
            // Well the index should always exist here, but if we didn't find it while checking the state,
            // then we obviously don't want to use it.
        }
        return state == InternalIndexState.ONLINE;
    }

    private NodeLabelIndexLookupImpl() {}
}
