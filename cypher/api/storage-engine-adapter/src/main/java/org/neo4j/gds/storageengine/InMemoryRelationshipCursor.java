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

import org.neo4j.gds.api.AdjacencyCursor;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.cypher.RelationshipIds;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.token.TokenHolders;

import java.util.List;

public abstract class InMemoryRelationshipCursor extends RelationshipRecord implements RelationshipVisitor<RuntimeException>, StorageRelationshipCursor {

    protected final CypherGraphStore graphStore;
    protected final TokenHolders tokenHolders;
    private final List<RelationshipIds.RelationshipIdContext> relationshipIdContexts;
    private final AdjacencyCursor[] adjacencyCursorCache;

    protected long sourceId;
    protected long targetId;
    private AdjacencyCursor adjacencyCursor;
    private int relationshipTypeOffset;
    private int relationshipContextIndex;

    public InMemoryRelationshipCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID);
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
        this.relationshipIdContexts = this.graphStore.relationshipIds().relationshipIdContexts();
        this.adjacencyCursorCache = relationshipIdContexts.stream().map(context -> {
                var graph = (CSRGraph) context.graph();
                return graph.relationshipTopologies().get(context.relationshipType()).adjacencyList().rawAdjacencyCursor();
            }
        ).toArray(AdjacencyCursor[]::new);
    }

    @Override
    public void visit(long relationshipId, int typeId, long startNodeId, long endNodeId) throws RuntimeException {
    }

    @Override
    public int type() {
        return getType();
    }

    @Override
    public long sourceNodeReference() {
        return sourceId;
    }

    @Override
    public long targetNodeReference() {
        return targetId;
    }

    @Override
    public boolean hasProperties() {
        return relationshipIdContexts.get(relationshipContextIndex).graph().hasRelationshipProperty();
    }

    @Override
    public long entityReference() {
        return getId();
    }

    @Override
    public boolean next() {
        while (true) {
            if (adjacencyCursor == null || !adjacencyCursor.hasNextVLong()) {
                if (!progressToNextContext()) {
                    return false;
                }
            } else {
                targetId = adjacencyCursor.nextVLong();
                setId(getId() + 1);
                return true;
            }
        }
    }

    @Override
    public void reset() {
        relationshipContextIndex = -1;
        relationshipTypeOffset = 0;
        adjacencyCursor = null;
        targetId = NO_ID;
        sourceId = NO_ID;
        setId(NO_ID);
    }

    public void resetCursors() {
        relationshipContextIndex = -1;
        relationshipTypeOffset = 0;
        adjacencyCursor = null;
        setId(NO_ID);
    }

    @Override
    public void setForceLoad() {

    }

    @Override
    public void close() {

    }

    private boolean progressToNextContext() {
        relationshipContextIndex++;

        if (relationshipContextIndex >= relationshipIdContexts.size()) {
            return false;
        }

        if (relationshipContextIndex > 0) {
            relationshipTypeOffset += relationshipIdContexts.get(relationshipContextIndex - 1).relationshipCount();
        }

        var context = relationshipIdContexts.get(relationshipContextIndex);

        var relationshipType = context.relationshipType();

        setType(tokenHolders.relationshipTypeTokens().getIdByName(relationshipType.name));

        var graph = (CSRGraph) context.graph();
        var reuseCursor = adjacencyCursorCache[relationshipContextIndex];

        this.adjacencyCursor = graph
            .relationshipTopologies()
            .get(relationshipType)
            .adjacencyList()
            .adjacencyCursor(reuseCursor, this.sourceId);

        setId(relationshipTypeOffset + context.offsets().get(sourceId) - 1);

        return true;
    }
}
