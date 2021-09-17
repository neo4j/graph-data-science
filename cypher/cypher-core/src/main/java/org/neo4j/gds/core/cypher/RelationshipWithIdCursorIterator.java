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
package org.neo4j.gds.core.cypher;

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.RelationshipCursor;

import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

public class RelationshipWithIdCursorIterator implements Iterator<CypherRelationshipCursor> {

    private final List<RelationshipIds.RelationshipIdContext> relationshipIdContexts;
    private final long nodeId;
    private final Predicate<RelationshipType> relationshipTypePredicate;

    private long relTypeOffset;
    private long relationshipId;
    private int contextCounter;
    private Iterator<RelationshipCursor> currentRelCursor;
    private RelationshipIds.RelationshipIdContext currentRelContext;

    RelationshipWithIdCursorIterator(
        List<RelationshipIds.RelationshipIdContext> relationshipIdContexts,
        long nodeId,
        Predicate<RelationshipType> relationshipTypePredicate
    ) {
        this.relationshipIdContexts = relationshipIdContexts;
        this.nodeId = nodeId;
        this.relationshipTypePredicate = relationshipTypePredicate;
        this.relTypeOffset = -1L;
        this.contextCounter = -1;
    }

    @Override
    public boolean hasNext() {
        if (currentRelCursor == null) {
            if (contextCounter == relationshipIdContexts.size() - 1) {
                return false;
            }
            progressToNextContext();
        }
        // either relationship predicate fails or rel cursor has no adjacency for `nodeId`
        if (!this.relationshipTypePredicate.test(currentRelContext.relationshipType()) || !currentRelCursor.hasNext()) {
            this.currentRelCursor = null;
            return hasNext();
        }

        return true;
    }

    @Override
    public CypherRelationshipCursor next() {
        var relationshipCursor = currentRelCursor.next();
        return ImmutableRelationshipWithIdCursor.of(
            relationshipCursor.sourceId(),
            relationshipCursor.targetId(),
            relationshipCursor.property(),
            relationshipId++
        );
    }

    private void progressToNextContext() {
        if (this.relTypeOffset == -1L) {
            this.relTypeOffset = 0L;
        } else {
            this.relTypeOffset += currentRelContext.relationshipCount();
        }

        this.contextCounter++;
        this.currentRelContext = relationshipIdContexts.get(contextCounter);
        this.currentRelCursor = currentRelContext
            .graph()
            .streamRelationships(nodeId, Double.NaN)
            .iterator();

        this.relationshipId = relTypeOffset + currentRelContext.offsets().get(nodeId);
    }

}
