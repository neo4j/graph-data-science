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

import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.cypher.CypherRelationshipCursor;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.token.TokenHolders;

import java.util.Iterator;

public abstract class InMemoryRelationshipCursor extends RelationshipRecord implements RelationshipVisitor<RuntimeException>, StorageRelationshipCursor {

    protected final CypherGraphStore graphStore;
    protected final TokenHolders tokenHolders;

    protected Iterator<CypherRelationshipCursor> relationshipCursors;
    protected CypherRelationshipCursor currentRelationshipCursor;

    public InMemoryRelationshipCursor(CypherGraphStore graphStore, TokenHolders tokenHolders, long id) {
        super(id);
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
    }

    @Override
    public void visit(long relationshipId, int typeId, long startNodeId, long endNodeId) throws RuntimeException {
    }

    @Override
    public int type() {
        return 0;
    }

    @Override
    public long sourceNodeReference() {
        return currentRelationshipCursor.sourceId();
    }

    @Override
    public long targetNodeReference() {
        return currentRelationshipCursor.targetId();
    }

    @Override
    public boolean hasProperties() {
        return false;
    }

    @Override
    public long entityReference() {
        return getId();
    }

    @Override
    public boolean next() {
        if (relationshipCursors.hasNext()) {
            currentRelationshipCursor = relationshipCursors.next();
            setId(currentRelationshipCursor.id());
            return true;
        }
        return false;
    }

    @Override
    public void reset() {
        relationshipCursors = null;
        currentRelationshipCursor = null;
    }

    @Override
    public void setForceLoad() {

    }

    @Override
    public void close() {

    }
}
