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
package org.neo4j.gds.compat;

import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.cypher.CypherRelationshipCursor;
import org.neo4j.gds.core.cypher.SingleElementIterator;
import org.neo4j.gds.storageengine.InMemoryRelationshipCursor;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.token.TokenHolders;

import java.util.stream.LongStream;
import java.util.stream.StreamSupport;

public abstract class AbstractInMemoryRelationshipScanCursor extends InMemoryRelationshipCursor implements StorageRelationshipScanCursor {

    public AbstractInMemoryRelationshipScanCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(graphStore, tokenHolders, NO_ID);
    }

    @Override
    public void scan() {
        relationshipCursors = LongStream.range(0, graphStore.nodeCount())
            .mapToObj(nodeId -> graphStore.relationshipIds().relationshipCursors(nodeId, RelationshipSelection.ALL_RELATIONSHIPS))
            .map(cursor -> (Iterable<CypherRelationshipCursor>) () -> cursor)
            .flatMap(relationshipCursors -> StreamSupport.stream(relationshipCursors.spliterator(), false))
            .iterator();
    }

    @Override
    public boolean scanBatch(AllRelationshipsScan scan, int sizeHint) {
        return false;
    }

    @Override
    public void single(long reference) {
        relationshipCursors = new SingleElementIterator<>(graphStore.relationshipIds().relationshipForId(reference));
    }
}
