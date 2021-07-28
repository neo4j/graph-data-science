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
package org.neo4j.gds.storageengine;

import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StorageRelationshipTraversalCursor;
import org.neo4j.token.TokenHolders;

public class InMemoryRelationshipTraversalCursor extends InMemoryRelationshipCursor implements StorageRelationshipTraversalCursor, RelationshipVisitor<RuntimeException> {

    private long originNodeReference;

    public InMemoryRelationshipTraversalCursor(GraphStore graphStore, TokenHolders tokenHolders) {
        super(graphStore, tokenHolders, NO_ID);
    }

    @Override
    public long neighbourNodeReference() {
        long source = sourceNodeReference();
        long target = targetNodeReference();
        if (source == originNodeReference) {
            return target;
        } else if (target == originNodeReference) {
            return source;
        } else {
            throw new IllegalStateException("NOT PART OF CHAIN");
        }
    }

    @Override
    public long originNodeReference() {
        return originNodeReference;
    }

    @Override
    public void init(long nodeReference, long reference, RelationshipSelection selection) {
        RelationshipType[] filteredRelTypes = graphStore.relationshipTypes().stream().filter(relType -> {
            int typeId = tokenHolders.relationshipTypeTokens().getIdByName(relType.name());
            return selection.test(typeId);
        }).toArray(RelationshipType[]::new);
        relationshipCursors = graphStore
            .getGraph(filteredRelTypes)
            .streamRelationships(nodeReference, Double.NaN)
            .iterator();
        originNodeReference = nodeReference;
    }
}
