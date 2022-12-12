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
package org.neo4j.gds.api;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.graph.GraphProperty;
import org.neo4j.gds.api.properties.graph.GraphPropertyValues;
import org.neo4j.gds.api.properties.nodes.NodeProperty;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.Direction;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.DeletionResult;
import org.neo4j.gds.core.loading.SingleTypeRelationshipImportResult;
import org.neo4j.values.storable.NumberType;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public abstract class GraphStoreAdapter implements GraphStoreWrapper {

    private final GraphStore graphStore;

    protected GraphStoreAdapter(GraphStore graphStore) {
        this.graphStore = graphStore;
    }

    @Override
    public GraphStore innerGraphStore() {
        return graphStore;
    }

    @Override
    public DatabaseId databaseId() {
        return graphStore.databaseId();
    }

    @Override
    public Capabilities capabilities() {
        return graphStore.capabilities();
    }

    @Override
    public GraphSchema schema() {
        return graphStore.schema();
    }

    @Override
    public ZonedDateTime modificationTime() {
        return graphStore.modificationTime();
    }


    @Override
    public Set<String> graphPropertyKeys() {
        return graphStore.graphPropertyKeys();
    }

    @Override
    public boolean hasGraphProperty(String propertyKey) {
        return graphStore.hasGraphProperty(propertyKey);
    }

    @Override
    public GraphProperty graphProperty(String propertyKey) {
        return graphStore.graphProperty(propertyKey);
    }

    @Override
    public ValueType graphPropertyType(String propertyKey) {
        return graphStore.graphPropertyType(propertyKey);
    }

    @Override
    public GraphPropertyValues graphPropertyValues(String propertyKey) {
        return graphStore.graphPropertyValues(propertyKey);
    }

    @Override
    public void addGraphProperty(String propertyKey, GraphPropertyValues propertyValues) {
        graphStore.addGraphProperty(propertyKey, propertyValues);
    }

    @Override
    public void removeGraphProperty(String propertyKey) {
        graphStore.removeGraphProperty(propertyKey);
    }

    @Override
    public long nodeCount() {
        return graphStore.nodeCount();
    }

    @Override
    public IdMap nodes() {
        return graphStore.nodes();
    }

    @Override
    public Set<NodeLabel> nodeLabels() {
        return graphStore.nodeLabels();
    }

    @Override
    public Set<String> nodePropertyKeys(NodeLabel label) {
        return graphStore.nodePropertyKeys(label);
    }

    @Override
    public Set<String> nodePropertyKeys() {
        return graphStore.nodePropertyKeys();
    }

    @Override
    public boolean hasNodeProperty(String propertyKey) {
        return graphStore.hasNodeProperty(propertyKey);
    }

    @Override
    public boolean hasNodeProperty(NodeLabel label, String propertyKey) {
        return graphStore.hasNodeProperty(label, propertyKey);
    }

    @Override
    public boolean hasNodeProperty(Collection<NodeLabel> labels, String propertyKey) {
        return graphStore.hasNodeProperty(labels, propertyKey);
    }

    @Override
    public Collection<String> nodePropertyKeys(Collection<NodeLabel> labels) {
        return graphStore.nodePropertyKeys(labels);
    }

    @Override
    public NodeProperty nodeProperty(String propertyKey) {
        return graphStore.nodeProperty(propertyKey);
    }

    @Override
    public void addNodeProperty(
       Set<NodeLabel> nodeLabels, String propertyKey, NodePropertyValues propertyValues
    ) {
        graphStore.addNodeProperty(nodeLabels, propertyKey, propertyValues);
    }

    @Override
    public void removeNodeProperty(String propertyKey) {
        graphStore.removeNodeProperty(propertyKey);
    }

    @Override
    public long relationshipCount() {
        return graphStore.relationshipCount();
    }

    @Override
    public long relationshipCount(RelationshipType relationshipType) {
        return graphStore.relationshipCount(relationshipType);
    }

    @Override
    public Set<RelationshipType> relationshipTypes() {
        return graphStore.relationshipTypes();
    }

    @Override
    public boolean hasRelationshipType(RelationshipType relationshipType) {
        return graphStore.hasRelationshipType(relationshipType);
    }

    @Override
    public boolean hasRelationshipProperty(RelationshipType relType, String propertyKey) {
        return graphStore.hasRelationshipProperty(relType, propertyKey);
    }

    @Override
    public Collection<String> relationshipPropertyKeys(Collection<RelationshipType> relTypes) {
        return graphStore.relationshipPropertyKeys(relTypes);
    }

    @Override
    public ValueType relationshipPropertyType(String propertyKey) {
        return graphStore.relationshipPropertyType(propertyKey);
    }

    @Override
    public Set<String> relationshipPropertyKeys() {
        return graphStore.relationshipPropertyKeys();
    }

    @Override
    public Set<String> relationshipPropertyKeys(RelationshipType relationshipType) {
        return graphStore.relationshipPropertyKeys(relationshipType);
    }

    @Override
    public RelationshipProperty relationshipPropertyValues(
        RelationshipType relationshipType, String propertyKey
    ) {
        return graphStore.relationshipPropertyValues(relationshipType, propertyKey);
    }

    @Override
    public void addRelationshipType(
        RelationshipType relationshipType,
        Optional<String> relationshipPropertyKey,
        Optional<NumberType> relationshipPropertyType,
        Direction direction,
        Relationships relationships
    ) {
        graphStore.addRelationshipType(
            relationshipType,
            relationshipPropertyKey,
            relationshipPropertyType,
            direction,
            relationships
        );
    }

    @Override
    public void addRelationshipType(
        RelationshipType relationshipType,
        SingleTypeRelationshipImportResult relationships
    ) {
        graphStore.addRelationshipType(relationshipType, relationships);
    }

    @Override
    public DeletionResult deleteRelationships(RelationshipType relationshipType) {
        return graphStore.deleteRelationships(relationshipType);
    }

    @Override
    public Graph getGraph(NodeLabel nodeLabel) {
        return graphStore.getGraph(nodeLabel);
    }

    @Override
    public Graph getGraph(Collection<NodeLabel> nodeLabels) {
        return graphStore.getGraph(nodeLabels);
    }

    @Override
    public Graph getGraph(RelationshipType... relationshipType) {
        return graphStore.getGraph(relationshipType);
    }

    @Override
    public Graph getGraph(String relationshipProperty) {
        return graphStore.getGraph(relationshipProperty);
    }

    @Override
    public Graph getGraph(
        RelationshipType relationshipType, Optional<String> relationshipProperty
    ) {
        return graphStore.getGraph(relationshipType, relationshipProperty);
    }

    @Override
    public Graph getGraph(
        Collection<RelationshipType> relationshipTypes, Optional<String> maybeRelationshipProperty
    ) {
        return graphStore.getGraph(relationshipTypes, maybeRelationshipProperty);
    }

    @Override
    public Graph getGraph(
        String nodeLabel, String relationshipType, Optional<String> maybeRelationshipProperty
    ) {
        return graphStore.getGraph(nodeLabel, relationshipType, maybeRelationshipProperty);
    }

    @Override
    public Graph getGraph(
        NodeLabel nodeLabel, RelationshipType relationshipType, Optional<String> maybeRelationshipProperty
    ) {
        return graphStore.getGraph(nodeLabel, relationshipType, maybeRelationshipProperty);
    }

    @Override
    public Graph getGraph(
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        Optional<String> maybeRelationshipProperty
    ) {
        return graphStore.getGraph(nodeLabels, relationshipTypes, maybeRelationshipProperty);
    }

    @Override
    public Graph getUnion() {
        return graphStore.getUnion();
    }

    @Override
    public CompositeRelationshipIterator getCompositeRelationshipIterator(
        RelationshipType relationshipType, List<String> propertyKeys
    ) {
        return graphStore.getCompositeRelationshipIterator(relationshipType, propertyKeys);
    }

    @Override
    public void canRelease(boolean canRelease) {
        graphStore.canRelease(canRelease);
    }

    @Override
    public void release() {
        graphStore.release();
    }
}
