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
import org.neo4j.gds.storageengine.InMemoryRelationshipCursor;
import org.neo4j.storageengine.api.AllRelationshipsScan;
import org.neo4j.storageengine.api.StorageRelationshipScanCursor;
import org.neo4j.token.TokenHolders;

public abstract class AbstractInMemoryRelationshipScanCursor extends InMemoryRelationshipCursor implements StorageRelationshipScanCursor {


    public AbstractInMemoryRelationshipScanCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(graphStore, tokenHolders);
    }

    @Override
    public void scan() {
        reset();
        sourceId = 0;
    }

    @Override
    public boolean next() {
        if (super.next()) {
            return true;
        } else {
            this.sourceId++;
            if (this.sourceId >= graphStore.nodeCount()) {
                return false;
            } else {
               resetCursors();
               return next();
            }
        }
    }

    @Override
    public boolean scanBatch(AllRelationshipsScan scan, int sizeHint) {
        return false;
    }

    @Override
    public void single(long reference) {
        reset();

        graphStore.relationshipIds().resolveRelationshipId(reference, (nodeId, offset, context) -> {
            this.sourceId = nodeId;
            setType(tokenHolders.relationshipTypeTokens().getIdByName(context.relationshipType().name));

            for (long i = 0; i < offset; i++) {
                next();
            }

            return null;
        });
    }
}
