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
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.api.Relationships;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.loading.DeletionResult;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.storable.NumberType;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
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
    public NamedDatabaseId databaseId() {
        return graphStore.databaseId();
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
    public long nodeCount() {
        return graphStore.nodeCount();
    }

    @Override
    public NodeMapping nodes() {
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
    public Map<NodeLabel, Set<String>> nodePropertyKeys() {
        return graphStore.nodePropertyKeys();
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
    public ValueType nodePropertyType(
        NodeLabel label, String propertyKey
    ) {
        return graphStore.nodePropertyType(label, propertyKey);
    }

    @Override
    public PropertyState nodePropertyState(String propertyKey) {
        return graphStore.nodePropertyState(propertyKey);
    }

    @Override
    public NodeProperties nodePropertyValues(String propertyKey) {
        return graphStore.nodePropertyValues(propertyKey);
    }

    @Override
    public NodeProperties nodePropertyValues(
        NodeLabel label, String propertyKey
    ) {
        return graphStore.nodePropertyValues(propertyKey);
    }

    @Override
    public void addNodeProperty(
        NodeLabel nodeLabel, String propertyKey, NodeProperties propertyValues
    ) {
        graphStore.addNodeProperty(nodeLabel, propertyKey, propertyValues);
    }

    @Override
    public void removeNodeProperty(NodeLabel nodeLabel, String propertyKey) {
        graphStore.removeNodeProperty(nodeLabel, propertyKey);
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
    public boolean isUndirected(RelationshipType relationshipType) {
        return graphStore.isUndirected(relationshipType);
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
        Relationships relationships
    ) {
        graphStore.addRelationshipType(relationshipType, relationshipPropertyKey, relationshipPropertyType, relationships);
    }

    @Override
    public DeletionResult deleteRelationships(RelationshipType relationshipType) {
        return graphStore.deleteRelationships(relationshipType);
    }

    @Override
    public Graph getGraph(RelationshipType... relationshipType) {
        return graphStore.getGraph(relationshipType);
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
