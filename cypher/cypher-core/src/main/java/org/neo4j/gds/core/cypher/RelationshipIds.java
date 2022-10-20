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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.AdjacencyList;
import org.neo4j.gds.api.AdjacencyProperties;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.token.TokenHolders;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public final class RelationshipIds extends CypherGraphStore.StateVisitor.Adapter {

    private final GraphStore graphStore;
    private final TokenHolders tokenHolders;
    private final List<RelationshipIdContext> relationshipIdContexts;
    private final List<UpdateListener> updateListeners;

    public interface UpdateListener {
        void onRelationshipIdsAdded(RelationshipIdContext relationshipIdContext);
    }

    static RelationshipIds fromGraphStore(GraphStore graphStore, TokenHolders tokenHolders) {
        var relationshipIdContexts = new ArrayList<RelationshipIdContext>(graphStore.relationshipTypes().size());

        graphStore.relationshipTypes()
            .stream()
            .map(relType -> relationshipIdContextFromRelType(graphStore, tokenHolders, relType))
            .forEach(relationshipIdContexts::add);
        return new RelationshipIds(graphStore, tokenHolders, relationshipIdContexts);
    }

    private RelationshipIds(GraphStore graphStore, TokenHolders tokenHolders, List<RelationshipIdContext> relationshipIdContexts) {
        this.graphStore = graphStore;
        this.tokenHolders = tokenHolders;
        this.relationshipIdContexts = relationshipIdContexts;
        this.updateListeners = new ArrayList<>();
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

    public void registerUpdateListener(UpdateListener updateListener) {
        this.updateListeners.add(updateListener);
        // replay added relationship id contexts
        relationshipIdContexts.forEach(updateListener::onRelationshipIdsAdded);
    }

    public void removeUpdateListener(UpdateListener updateListener) {
        this.updateListeners.remove(updateListener);
    }

    @Override
    public void relationshipTypeAdded(String relationshipType) {
        var relationshipIdContext = relationshipIdContextFromRelType(graphStore, tokenHolders, RelationshipType.of(relationshipType));
        relationshipIdContexts.add(relationshipIdContext);
        updateListeners.forEach(updateListener -> updateListener.onRelationshipIdsAdded(relationshipIdContext));
    }

    @NotNull
    private static RelationshipIdContext relationshipIdContextFromRelType(
        GraphStore graphStore,
        TokenHolders tokenHolders,
        RelationshipType relType
    ) {
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

        return new RelationshipIdContext(
            relType,
            relTypeId,
            relCount,
            graph,
            offsets,
            propertyIds,
            adjacencyProperties
        );
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

    public static class RelationshipIdContext {
        private final RelationshipType relationshipType;
        private final int relationshipTypeId;
        private final long relationshipCount;
        private final CSRGraph graph;
        private final HugeLongArray offsets;
        private final int[] propertyIds;
        private final AdjacencyProperties[] adjacencyProperties;
        private final AdjacencyList adjacencyList;

        RelationshipIdContext(
            RelationshipType relationshipType,
            int relationshipTypeId,
            long relationshipCount,
            CSRGraph graph,
            HugeLongArray offsets,
            int[] propertyIds,
            AdjacencyProperties[] adjacencyProperties
        ) {
            this.relationshipType = relationshipType;
            this.relationshipTypeId = relationshipTypeId;
            this.relationshipCount = relationshipCount;
            this.graph = graph;
            this.offsets = offsets;
            this.propertyIds = propertyIds;
            this.adjacencyProperties = adjacencyProperties;
            this.adjacencyList = graph().relationshipTopologies().get(relationshipType()).adjacencyList();
        }

        public RelationshipType relationshipType() {
            return relationshipType;
        }

        public int relationshipTypeId() {
            return relationshipTypeId;
        }

        public long relationshipCount() {
            return relationshipCount;
        }

        public CSRGraph graph() {
            return graph;
        }

        public HugeLongArray offsets() {
            return offsets;
        }

        public int[] propertyIds() {
            return propertyIds;
        }

        public AdjacencyProperties[] adjacencyProperties() {
            return adjacencyProperties;
        }

        public AdjacencyList adjacencyList() {
            return adjacencyList;
        }
    }
}
