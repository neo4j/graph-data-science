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
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.storageengine.api.RelationshipSelection;
import org.neo4j.token.TokenHolders;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class RelationshipIds {

    private final List<RelationshipIdContext> relationshipIdContexts;
    private final TokenHolders tokenHolders;

    public static RelationshipIds fromGraphStore(GraphStore graphStore, TokenHolders tokenHolders) {
        var relationshipIdContexts = new ArrayList<RelationshipIdContext>();
        graphStore.relationshipTypes().forEach(relType -> {
            var relCount = graphStore.relationshipCount(relType);
            var graph = graphStore.getGraph(relType);
            var offsets = HugeLongArray.newArray(graph.nodeCount(), AllocationTracker.empty());

            long offset = 0L;
            for (long i = 0; i < offsets.size(); i++) {
                offsets.set(i, offset);
                offset += graph.degree(i);
            }
            relationshipIdContexts.add(ImmutableRelationshipIdContext.of(relType, relCount, graph, offsets));
        });
        return new RelationshipIds(relationshipIdContexts, tokenHolders);
    }

    private RelationshipIds(
        List<RelationshipIdContext> relationshipIdContexts,
        TokenHolders tokenHolders
    ) {
        this.relationshipIdContexts = relationshipIdContexts;
        this.tokenHolders = tokenHolders;
    }

    public RelationshipWithIdCursor relationshipForId(long relationshipId) {
        long id = relationshipId;
        for (RelationshipIdContext relationshipIdContext : relationshipIdContexts) {
            if (id - relationshipIdContext.relationshipCount() >= 0) {
                id -= relationshipIdContext.relationshipCount();
            } else {
                var nodeId = relationshipIdContext.offsets().binarySearch(id);
                var offsetInAdjacency = id - relationshipIdContext.offsets().get(nodeId);
                return relationshipIdContext
                    .graph()
                    .streamRelationships(nodeId, Double.NaN)
                    .skip(offsetInAdjacency)
                    .findFirst()
                    .map(relCursor -> RelationshipWithIdCursor.fromRelationshipCursor(relCursor, relationshipId))
                    .orElseThrow(IllegalStateException::new);
            }
        }

        throw new IllegalArgumentException(formatWithLocale("No relationship with id %d was found.", relationshipId));
    }

    public Iterator<RelationshipWithIdCursor> relationshipCursors(long nodeId, RelationshipSelection relationshipSelection) {
        Predicate<RelationshipType> relationshipSelectionPredicate = relationshipType -> {
            var relTypeToken = tokenHolders.relationshipTypeTokens().getIdByName(relationshipType.name());
            return relationshipSelection.test(relTypeToken);
        };
        return new RelationshipWithIdCursorIterator(relationshipIdContexts, nodeId, relationshipSelectionPredicate);
    }


    @ValueClass
    public interface RelationshipIdContext {
        RelationshipType relationshipType();
        long relationshipCount();
        Graph graph();
        HugeLongArray offsets();
    }
}
