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
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.PropertyCursor;
import org.neo4j.gds.compat.AbstractInMemoryRelationshipPropertyCursor;
import org.neo4j.gds.compat.InMemoryPropertySelection;
import org.neo4j.gds.core.cypher.CypherGraphStore;
import org.neo4j.gds.core.cypher.RelationshipIds;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.storageengine.api.RelationshipVisitor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.StorageRelationshipCursor;
import org.neo4j.token.TokenHolders;

import java.util.Arrays;

public abstract class InMemoryRelationshipCursor extends RelationshipRecord implements RelationshipVisitor<RuntimeException>, StorageRelationshipCursor {

    protected final CypherGraphStore graphStore;
    protected final TokenHolders tokenHolders;
    private final RelationshipIds.RelationshipIdContext[] relationshipIdContexts;
    private final AdjacencyCursor[] adjacencyCursorCache;
    private final PropertyCursor[][] propertyCursorCache;

    protected long sourceId;
    protected long targetId;
    protected RelationshipSelection selection;

    private AdjacencyCursor adjacencyCursor;
    private PropertyCursor[] propertyCursors;
    private final double[] propertyValuesCache;
    private int relationshipTypeOffset;
    private int relationshipContextIndex;
    private int[] propertyIds;

    public InMemoryRelationshipCursor(CypherGraphStore graphStore, TokenHolders tokenHolders) {
        super(NO_ID);
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
        this.relationshipIdContexts = this.graphStore.relationshipIds().relationshipIdContexts();
        this.adjacencyCursorCache = Arrays.stream(relationshipIdContexts)
            .map(context -> context.adjacencyList().rawAdjacencyCursor())
            .toArray(AdjacencyCursor[]::new);

        this.propertyCursorCache = Arrays.stream(relationshipIdContexts)
            .map(context -> Arrays
                .stream(context.adjacencyProperties())
                .map(AdjacencyProperties::rawPropertyCursor)
                .toArray(PropertyCursor[]::new)
            )
            .toArray(PropertyCursor[][]::new);

        var maxPropertySize = Arrays.stream(propertyCursorCache).mapToInt(a -> a.length).max().orElse(0);
        this.propertyValuesCache = new double[maxPropertySize];
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
        return relationshipIdContexts[relationshipContextIndex].graph().hasRelationshipProperty();
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

                for (int i = 0; i < propertyCursors.length; i++) {
                    propertyValuesCache[i] = Double.longBitsToDouble(propertyCursors[i].nextLong());
                }

                return true;
            }
        }
    }

    @Override
    public void reset() {
        this.relationshipContextIndex = -1;
        this.relationshipTypeOffset = 0;
        this.adjacencyCursor = null;
        this.targetId = NO_ID;
        this.sourceId = NO_ID;
        this.selection = null;
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

    public void properties(StoragePropertyCursor propertyCursor, InMemoryPropertySelection selection) {
        var inMemoryCursor = (AbstractInMemoryRelationshipPropertyCursor) propertyCursor;
        inMemoryCursor.initRelationshipPropertyCursor(this.sourceId, propertyIds, propertyValuesCache, selection);
    }

    private boolean progressToNextContext() {
        relationshipContextIndex++;

        if (relationshipContextIndex >= relationshipIdContexts.length) {
            return false;
        }

        if (relationshipContextIndex > 0) {
            relationshipTypeOffset += relationshipIdContexts[relationshipContextIndex - 1].relationshipCount();
        }

        var context = relationshipIdContexts[relationshipContextIndex];

        if (!selection.test(context.relationshipTypeId())) {
            return progressToNextContext();
        }

        setType(context.relationshipTypeId());

        // initialize the adjacency cursor
        var reuseCursor = adjacencyCursorCache[relationshipContextIndex];
        this.adjacencyCursor = context
            .adjacencyList()
            .adjacencyCursor(reuseCursor, this.sourceId);

        // initialize the property cursors
        this.propertyIds = context.propertyIds();
        this.propertyCursors = propertyCursorCache[relationshipContextIndex];
        var adjacencyProperties = context.adjacencyProperties();
        for (int i = 0; i < propertyCursors.length; i++) {
                adjacencyProperties[i].propertyCursor(propertyCursors[i], this.sourceId);
        }

        setId(relationshipTypeOffset + context.offsets().get(sourceId) - 1);

        return true;
    }
}
