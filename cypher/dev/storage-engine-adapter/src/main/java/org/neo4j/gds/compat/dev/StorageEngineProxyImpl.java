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
package org.neo4j.gds.compat.dev;

import org.neo4j.counts.CountsAccessor;
import org.neo4j.counts.CountsStore;
import org.neo4j.gds.compat.StorageEngineProxyApi;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphdb.Direction;
import org.neo4j.internal.recordstorage.InMemoryStorageReaderImpl;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.storageengine.api.CommandCreationContext;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageReader;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.token.TokenHolders;

public class StorageEngineProxyImpl implements StorageEngineProxyApi {

    @Override
    public InMemoryStorageEngineImpl.Builder inMemoryStorageEngineBuilder(
        DatabaseLayout databaseLayout, TokenHolders tokenHolders
    ) {
        return new InMemoryStorageEngineImpl.Builder(databaseLayout, tokenHolders);
    }

    @Override
    public CountsStore inMemoryCountsStore(
        GraphStore graphStore, TokenHolders tokenHolders
    ) {
        return new InMemoryCountsStoreImpl(graphStore, tokenHolders);
    }

    @Override
    public CommandCreationContext inMemoryCommandCreationContext() {
        return new InMemoryCommandCreationContextImpl();
    }

    @Override
    public void initRelationshipTraversalCursorForRelType(
        StorageRelationshipTraversalCursor cursor,
        long sourceNodeId,
        int relTypeToken
    ) {
        var relationshipSelection = RelationshipSelection.selection(
            relTypeToken,
            Direction.OUTGOING
        );
        cursor.init(sourceNodeId, -1, relationshipSelection);
    }

    @Override
    public StorageReader inMemoryStorageReader(
        GraphStore graphStore, TokenHolders tokenHolders, CountsAccessor counts
    ) {
        return new InMemoryStorageReaderImpl(graphStore, tokenHolders, counts);
    }
}
