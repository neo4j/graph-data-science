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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.Orientation;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.GraphStoreAdapter;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.token.TokenHolders;
import org.neo4j.values.storable.NumberType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class CypherGraphStore extends GraphStoreAdapter implements NodeLabelUpdater {

    private final CypherIdMap cypherIdMap;
    private final List<StateVisitor> stateVisitors;

    private RelationshipIds relationshipIds;

    public CypherGraphStore(GraphStore graphStore) {
        super(graphStore);
        this.cypherIdMap = new CypherIdMap(super.nodes());
        this.stateVisitors = new ArrayList<>();
    }

    public void initialize(TokenHolders tokenHolders) {
        this.relationshipIds = RelationshipIds.fromGraphStore(innerGraphStore(), tokenHolders);
        registerStateVisitor(relationshipIds);
    }

    public void registerStateVisitor(StateVisitor stateVisitor) {
        this.stateVisitors.add(stateVisitor);
    }

    @Override
    public Capabilities capabilities() {
        return ImmutableStaticCapabilities.builder().from(super.capabilities()).canWriteToDatabase(false).build();
    }

    @Override
    public IdMap nodes() {
        return this.cypherIdMap;
    }

    @Override
    public void addNodeLabel(NodeLabel nodeLabel) {
        this.cypherIdMap.addNodeLabel(nodeLabel);
    }

    @Override
    public void addLabelToNode(long nodeId, NodeLabel nodeLabel) {
        this.cypherIdMap.addLabelToNode(nodeId, nodeLabel);
    }

    @Override
    public void removeLabelFromNode(long nodeId, NodeLabel nodeLabel) {
        this.cypherIdMap.removeLabelFromNode(nodeId, nodeLabel);
    }

    @Override
    public Set<NodeLabel> nodeLabels() {
        return this.cypherIdMap.availableNodeLabels();
    }

    @Override
    public void removeNodeProperty(String propertyKey) {
        super.removeNodeProperty(propertyKey);
        stateVisitors.forEach(stateVisitor -> stateVisitor.nodePropertyRemoved(propertyKey));
    }

    @Override
    public void addNodeProperty(
        Set<NodeLabel> nodeLabels, String propertyKey, NodePropertyValues propertyValues
    ) {
        super.addNodeProperty(nodeLabels, propertyKey, propertyValues);
        stateVisitors.forEach(stateVisitor -> stateVisitor.nodePropertyAdded(propertyKey));
    }

    @Override
    public void addRelationshipType(
        RelationshipType relationshipType,
        Optional<String> relationshipPropertyKey,
        Optional<NumberType> relationshipPropertyType,
        Orientation orientation,
        Relationships relationships
    ) {
        super.addRelationshipType(relationshipType, relationshipPropertyKey, relationshipPropertyType, orientation, relationships);
        relationshipPropertyKey.ifPresent(
            propertyKey -> stateVisitors.forEach(stateVisitor -> stateVisitor.relationshipPropertyAdded(propertyKey))
        );
        stateVisitors.forEach(stateVisitor -> stateVisitor.relationshipTypeAdded(relationshipType.name()));
    }

    public RelationshipIds relationshipIds() {
        return this.relationshipIds;
    }

    public interface StateVisitor {
        void nodePropertyRemoved(String propertyKey);

        void nodePropertyAdded(String propertyKey);

        void nodeLabelAdded(String nodeLabel);

        void relationshipTypeAdded(String relationshipType);

        void relationshipPropertyAdded(String relationshipProperty);

        class Adapter implements StateVisitor {
            @Override
            public void nodePropertyRemoved(String propertyKey) {

            }

            @Override
            public void nodePropertyAdded(String propertyKey) {

            }

            @Override
            public void nodeLabelAdded(String nodeLabel) {

            }

            @Override
            public void relationshipTypeAdded(String relationshipType) {

            }

            @Override
            public void relationshipPropertyAdded(String relationshipProperty) {

            }
        }
    }
}
