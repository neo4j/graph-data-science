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
package org.neo4j.internal.recordstorage;

import org.neo4j.common.EntityType;
import org.neo4j.counts.CountsAccessor;
import org.neo4j.gds.compat._43.InMemoryNodeCursor;
import org.neo4j.gds.compat._43.InMemoryPropertyCursor;
import org.neo4j.gds.compat._43.InMemoryRelationshipScanCursor;
import org.neo4j.gds.compat._43.InMemoryRelationshipTraversalCursor;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.internal.schema.constraints.IndexBackedConstraintDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.token.TokenHolders;

import java.util.Collection;
import java.util.Collections;

public class InMemoryStorageReader43 extends AbstractInMemoryStorageReader {

    public InMemoryStorageReader43(
        CypherGraphStore graphStore,
        TokenHolders tokenHolders,
        CountsAccessor counts
    ) {
        super(graphStore, tokenHolders, counts);
    }

    @Override
    public Collection<IndexBackedConstraintDescriptor> uniquenessConstraintsGetRelated(
        long[] labels,
        int[] propertyKeyIds,
        EntityType entityType
    ) {
        return Collections.emptyList();
    }

    @Override
    public long relationshipsGetCount() {
        return graphStore.relationshipCount();
    }

    @Override
    public boolean nodeExists(long id, CursorContext cursorContext) {
        return super.nodeExists(id);
    }

    @Override
    public boolean relationshipExists(long id, CursorContext cursorContext) {
        return true;
    }

    @Override
    public StorageNodeCursor allocateNodeCursor(CursorContext cursorContext) {
        return new InMemoryNodeCursor(graphStore, tokenHolders);
    }

    @Override
    public StoragePropertyCursor allocatePropertyCursor(
        CursorContext cursorContext, MemoryTracker memoryTracker
    ) {
        return new InMemoryPropertyCursor(graphStore, tokenHolders);
    }

    @Override
    public StorageRelationshipTraversalCursor allocateRelationshipTraversalCursor(CursorContext cursorContext) {
        return new InMemoryRelationshipTraversalCursor(graphStore, tokenHolders);
    }

    @Override
    public StorageRelationshipScanCursor allocateRelationshipScanCursor(CursorContext cursorContext) {
        return new InMemoryRelationshipScanCursor(graphStore, tokenHolders);
    }

    @Override
    public AllRelationshipsScan allRelationshipScan() {
        return new AbstractAllRelationshipScan() {
            @Override
            boolean scanRange(AbstractInMemoryRelationshipScanCursor cursor, long start, long stopInclusive) {
                return cursor.scanRange(start, stopInclusive);
            }

            @Override
            public boolean scanBatch(int sizeHint, AbstractInMemoryRelationshipScanCursor cursor) {
                return super.scanBatch(sizeHint, cursor);
            }
        };
    }
}
