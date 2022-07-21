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
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.loading.DeletionResult;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.values.storable.NumberType;

import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Collections.singletonList;

public interface GraphStore {

    NamedDatabaseId databaseId();

    GraphSchema schema();

    ZonedDateTime modificationTime();

    long nodeCount();

    NodeMapping nodes();

    Set<NodeLabel> nodeLabels();

    Set<String> nodePropertyKeys(NodeLabel label);

    Map<NodeLabel, Set<String>> nodePropertyKeys();

    boolean hasNodeProperty(NodeLabel label, String propertyKey);

    boolean hasNodeProperty(Collection<NodeLabel> labels, String propertyKey);

    default Collection<String> nodePropertyKeys(Collection<NodeLabel> labels) {
        if (labels.isEmpty()) {
            return Collections.emptyList();
        }
        // intersection of propertyKeys
        Iterator<NodeLabel> iterator = labels.iterator();
        Set<String> result = nodePropertyKeys(iterator.next());
        while (iterator.hasNext()) {
            result.retainAll(nodePropertyKeys(iterator.next()));
        }

        return result;
    }

    ValueType nodePropertyType(NodeLabel label, String propertyKey);

    PropertyState nodePropertyState(String propertyKey);

    NodeProperties nodePropertyValues(String propertyKey);

    NodeProperties nodePropertyValues(NodeLabel label, String propertyKey);

    void addNodeProperty(
        NodeLabel nodeLabel,
        String propertyKey,
        NodeProperties propertyValues
    );

    void removeNodeProperty(NodeLabel nodeLabel, String propertyKey);

    long relationshipCount();

    long relationshipCount(RelationshipType relationshipType);

    Set<RelationshipType> relationshipTypes();

    boolean hasRelationshipType(RelationshipType relationshipType);

    boolean isUndirected(RelationshipType relationshipType);


    // Relationship Properties

    boolean hasRelationshipProperty(RelationshipType relType, String propertyKey);

    default Collection<String> relationshipPropertyKeys(Collection<RelationshipType> relTypes) {
        if (relTypes.isEmpty()) {
            return Collections.emptyList();
        }
        // intersection of propertyKeys
        Iterator<RelationshipType> iterator = relTypes.iterator();
        var result = new HashSet<>(relationshipPropertyKeys(iterator.next()));
        while (iterator.hasNext()) {
            result.retainAll(relationshipPropertyKeys(iterator.next()));
        }

        return result;
    }


    ValueType relationshipPropertyType(String propertyKey);

    Set<String> relationshipPropertyKeys();

    Set<String> relationshipPropertyKeys(RelationshipType relationshipType);

    RelationshipProperty relationshipPropertyValues(RelationshipType relationshipType, String propertyKey);

    void addRelationshipType(
        RelationshipType relationshipType,
        Optional<String> relationshipPropertyKey,
        Optional<NumberType> relationshipPropertyType,
        Relationships relationships
    );

    DeletionResult deleteRelationships(RelationshipType relationshipType);

    default Graph getGraph(RelationshipType... relationshipType) {
        return getGraph(nodeLabels(), Arrays.asList(relationshipType), Optional.empty());
    }

    default Graph getGraph(RelationshipType relationshipType, Optional<String> relationshipProperty) {
        return getGraph(nodeLabels(), singletonList(relationshipType), relationshipProperty);
    }

    default Graph getGraph(Collection<RelationshipType> relationshipTypes, Optional<String> maybeRelationshipProperty) {
        return getGraph(nodeLabels(), relationshipTypes, maybeRelationshipProperty);
    }

    default Graph getGraph(
        String nodeLabel,
        String relationshipType,
        Optional<String> maybeRelationshipProperty
    ) {
        return getGraph(NodeLabel.of(nodeLabel), RelationshipType.of(relationshipType), maybeRelationshipProperty);
    }

    default Graph getGraph(
        NodeLabel nodeLabel,
        RelationshipType relationshipType,
        Optional<String> maybeRelationshipProperty
    ) {
        return getGraph(List.of(nodeLabel), List.of(relationshipType), maybeRelationshipProperty);
    }

    Graph getGraph(
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        Optional<String> maybeRelationshipProperty
    );

    Graph getUnion();

    CompositeRelationshipIterator getCompositeRelationshipIterator(
        RelationshipType relationshipType,
        List<String> propertyKeys
    );

    void canRelease(boolean canRelease);

    void release();
}
