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
package org.neo4j.gds.similarity.nil;

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CompositeRelationshipIterator;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeMapping;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.RelationshipProperty;
import org.neo4j.gds.api.RelationshipPropertyStore;
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

/**
 * The NullGraphStore is used to store a {@link NullGraph}.
 * It helps non-product algos work under the standard API.
 */
public class NullGraphStore implements GraphStore {

    private final NamedDatabaseId databaseId;

    static class NullGraphException extends UnsupportedOperationException {

        NullGraphException() {
            super("This algorithm does not support operating on named graphs. " +
                  "Please report this stacktrace to https://github.com/neo4j/graph-data-science");
        }
    }

    public NullGraphStore(NamedDatabaseId databaseId) {
        this.databaseId = databaseId;
    }

    @Override
    public NamedDatabaseId databaseId() {
        return databaseId;
    }

    @Override
    public GraphSchema schema() {
        throw new NullGraphException();
    }

    @Override
    public ZonedDateTime modificationTime() {
        return ZonedDateTime.now();
    }

    @Override
    public long nodeCount() {
        return 0;
    }

    @Override
    public NodeMapping nodes() {
        throw new NullGraphException();
    }

    @Override
    public Set<NodeLabel> nodeLabels() {
        return Set.of();
    }

    @Override
    public Set<String> nodePropertyKeys(NodeLabel label) {
        return Set.of();
    }

    @Override
    public Map<NodeLabel, Set<String>> nodePropertyKeys() {
        return Map.of();
    }

    @Override
    public boolean hasNodeProperty(NodeLabel label, String propertyKey) {
        return false;
    }

    @Override
    public boolean hasNodeProperty(Collection<NodeLabel> labels, String propertyKey) {
        return false;
    }

    @Override
    public ValueType nodePropertyType(NodeLabel label, String propertyKey) {
        return ValueType.UNKNOWN;
    }

    @Override
    public PropertyState nodePropertyState(String propertyKey) {
        return PropertyState.TRANSIENT;
    }

    @Override
    public NodeProperties nodePropertyValues(String propertyKey) {
        throw new NullGraphException();
    }

    @Override
    public NodeProperties nodePropertyValues(NodeLabel label, String propertyKey) {
        throw new NullGraphException();
    }

    @Override
    public void addNodeProperty(
        NodeLabel nodeLabel,
        String propertyKey,
        NodeProperties propertyValues
    ) {}

    @Override
    public void removeNodeProperty(NodeLabel nodeLabel, String propertyKey) {}

    @Override
    public long relationshipCount() {
        return 0;
    }

    @Override
    public long relationshipCount(RelationshipType relationshipType) {
        return 0;
    }

    @Override
    public Set<RelationshipType> relationshipTypes() {
        return Set.of();
    }

    @Override
    public boolean hasRelationshipType(RelationshipType relationshipType) {
        return false;
    }

    @Override
    public boolean isUndirected(RelationshipType relationshipType) {
        return false;
    }

    @Override
    public boolean hasRelationshipProperty(RelationshipType relType, String propertyKey) {
        return false;
    }

    @Override
    public ValueType relationshipPropertyType(String propertyKey) {
        return ValueType.UNKNOWN;
    }

    @Override
    public Set<String> relationshipPropertyKeys() {
        return Set.of();
    }

    @Override
    public Set<String> relationshipPropertyKeys(RelationshipType relationshipType) {
        return Set.of();
    }

    @Override
    public RelationshipProperty relationshipPropertyValues(
        RelationshipType relationshipType, String propertyKey
    ) {
        return RelationshipPropertyStore.empty().get(propertyKey);
    }

    @Override
    public void addRelationshipType(
        RelationshipType relationshipType,
        Optional<String> relationshipPropertyKey,
        Optional<NumberType> relationshipPropertyType,
        Relationships relationships
    ) {}

    @Override
    public DeletionResult deleteRelationships(RelationshipType relationshipType) {
        return DeletionResult.of(c -> {});
    }

    @Override
    public Graph getGraph(
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        Optional<String> maybeRelationshipProperty
    ) {
        return new NullGraph();
    }

    @Override
    public Graph getUnion() {
        return new NullGraph();
    }

    @Override
    public CompositeRelationshipIterator getCompositeRelationshipIterator(
        RelationshipType relationshipType, List<String> propertyKeys
    ) {
        throw new NullGraphException();
    }

    @Override
    public void canRelease(boolean canRelease) {}

    @Override
    public void release() {}
}
