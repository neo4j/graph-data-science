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

import org.apache.commons.lang3.mutable.MutableInt;
import org.immutables.value.Value;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.token.TokenHolders;

import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class RelationshipIds {

    private final RelationshipIdContext[] relationshipIdContexts;

    static RelationshipIds fromGraphStore(GraphStore graphStore, TokenHolders tokenHolders) {
        var relationshipIdContexts = new RelationshipIdContext[graphStore.relationshipTypes().size()];
        var index = new MutableInt(0);

        graphStore.relationshipTypes().forEach(relType -> {
            var relCount = graphStore.relationshipCount(relType);
            var graph = (CSRGraph) graphStore.getGraph(relType);
            var offsets = computeAccumulatedOffsets(graph);
            int relTypeId = tokenHolders.relationshipTypeTokens().getIdByName(relType.name);

            List<RelationshipProperty> relationshipProperties = graphStore.relationshipPropertyKeys(relType)
                .stream()
                .map(relProperty -> graphStore.relationshipPropertyValues(relType, relProperty))
                .collect(Collectors.toList());

            int[] propertyIds = relationshipProperties
                .stream()
                .mapToInt(relationshipProperty -> tokenHolders
                    .propertyKeyTokens()
                    .getIdByName(relationshipProperty.key()))
                .toArray();

            AdjacencyProperties[] adjacencyProperties = relationshipProperties
                .stream()
                .map(relationshipProperty -> relationshipProperty.values().propertiesList())
                .toArray(AdjacencyProperties[]::new);

            relationshipIdContexts[index.getAndIncrement()] = ImmutableRelationshipIdContext.of(relType, relTypeId, relCount, graph, offsets, propertyIds, adjacencyProperties);
        });
        return new RelationshipIds(relationshipIdContexts);
    }

    private RelationshipIds(RelationshipIdContext[] relationshipIdContexts) {
        this.relationshipIdContexts = relationshipIdContexts;
    }

    public RelationshipIdContext[] relationshipIdContexts() {
        return relationshipIdContexts;
    }

    public <T> T resolveRelationshipId(long relationshipId, ResolvedRelationshipIdFunction<T> relationshipIdConsumer) {
        long graphLocalRelationshipId = relationshipId;
        // Find the correct RelationshipIdContext given a relationship id.
        // Relationship ids are created consecutively for each topology stored
        // in the GraphStore. For example if the topology of type `REL1`
        // has 42 relationships, then the first relationship id of the topology
        // of type `REL2` has a relationship id of 43, given that `REL2` comes after
        // `REL1`. This operation tries to reverse that logic.
        for (RelationshipIdContext relationshipIdContext : relationshipIdContexts) {
            // If the relationship id is greater than the relationship count of the current
            // context, we can be sure that it does not belong to the topology represented
            // by the current context.
            if (graphLocalRelationshipId >= relationshipIdContext.relationshipCount()) {
                // Subtract the relationship count of the current context to move
                // the relationship id into the range of the next context.
                graphLocalRelationshipId -= relationshipIdContext.relationshipCount();
            } else {
                // We have found the context that contains the relationship id.
                // Now we need to compute the exact position within the relationships
                // of that context.
                var nodeId = relationshipIdContext.offsets().binarySearch(graphLocalRelationshipId);
                var offsetInAdjacency = graphLocalRelationshipId - relationshipIdContext.offsets().get(nodeId);
                return relationshipIdConsumer.accept(nodeId, offsetInAdjacency, relationshipIdContext);
            }
        }
        throw new IllegalArgumentException(formatWithLocale("No relationship with id %d was found.", relationshipId));
    }

    private static HugeLongArray computeAccumulatedOffsets(Graph graph) {
        var offsets = HugeLongArray.newArray(graph.nodeCount());

        long offset = 0L;
        for (long i = 0; i < offsets.size(); i++) {
            offsets.set(i, offset);
            offset += graph.degree(i);
        }
        return offsets;
    }

    public interface ResolvedRelationshipIdFunction<T> {
        T accept(long nodeId, long offsetInAdjacency, RelationshipIdContext relationshipIdContext);
    }

    @ValueClass
    public interface RelationshipIdContext {
        RelationshipType relationshipType();
        int relationshipTypeId();
        long relationshipCount();
        CSRGraph graph();
        HugeLongArray offsets();
        int[] propertyIds();
        AdjacencyProperties[] adjacencyProperties();

        @Value.Derived
        default AdjacencyList adjacencyList() {
            return graph().relationshipTopologies().get(relationshipType()).adjacencyList();
        }
    }
}
