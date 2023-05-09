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
package org.neo4j.gds.compat._58;

import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.storageengine.api.StorageLocks;
import org.neo4j.storageengine.api.txstate.ReadableTransactionState;

public class InMemoryStorageLocksImpl implements StorageLocks {

    InMemoryStorageLocksImpl(ResourceLocker locker) {}

    @Override
    public void acquireExclusiveNodeLock(LockTracer lockTracer, long... ids) {}

    @Override
    public void releaseExclusiveNodeLock(long... ids) {}

    @Override
    public void acquireSharedNodeLock(LockTracer lockTracer, long... ids) {}

    @Override
    public void releaseSharedNodeLock(long... ids) {}

    @Override
    public void acquireExclusiveRelationshipLock(LockTracer lockTracer, long... ids) {}

    @Override
    public void releaseExclusiveRelationshipLock(long... ids) {}

    @Override
    public void acquireSharedRelationshipLock(LockTracer lockTracer, long... ids) {}

    @Override
    public void releaseSharedRelationshipLock(long... ids) {}

    @Override
    public void acquireRelationshipCreationLock(
        LockTracer lockTracer,
        long sourceNode,
        long targetNode,
        boolean sourceNodeAddedInTx,
        boolean targetNodeAddedInTx
    ) {
    }

    @Override
    public void acquireRelationshipDeletionLock(
        LockTracer lockTracer,
        long sourceNode,
        long targetNode,
        long relationship,
        boolean relationshipAddedInTx,
        boolean sourceNodeAddedInTx,
        boolean targetNodeAddedInTx
    ) {
    }

    @Override
    public void acquireNodeDeletionLock(
        ReadableTransactionState readableTransactionState,
        LockTracer lockTracer,
        long node
    ) {}

    @Override
    public void acquireNodeLabelChangeLock(LockTracer lockTracer, long node, int labelId) {}
}
