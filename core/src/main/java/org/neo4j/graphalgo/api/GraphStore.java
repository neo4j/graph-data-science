/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.api;

import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.annotation.ValueClass;
import org.neo4j.graphalgo.core.huge.HugeGraph;
import org.neo4j.graphalgo.core.loading.CSRGraphStore;
import org.neo4j.graphalgo.core.loading.DeletionResult;
import org.neo4j.graphalgo.core.schema.GraphStoreSchema;
import org.neo4j.values.storable.NumberType;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public interface GraphStore {

    enum PropertyState {
        PERSISTENT, TRANSIENT
    }

    ZonedDateTime modificationTime();

    LabeledIdMapping nodes();

    Set<NodeLabel> nodeLabels();

    Set<String> nodePropertyKeys(NodeLabel label);

    Map<NodeLabel, Set<String>> nodePropertyKeys();

    long nodePropertyCount();

    boolean hasNodeProperty(Collection<NodeLabel> labels, String propertyKey);

    void addNodeProperty(
        NodeLabel nodeLabel,
        String propertyKey,
        NumberType propertyType,
        NodeProperties propertyValues
    );

    void removeNodeProperty(NodeLabel nodeLabel, String propertyKey);

    // TODO: remove and replace by nodePropertyState()
    CSRGraphStore.NodeProperty nodeProperty(String propertyKey);

    NumberType nodePropertyType(String propertyKey);

    NodeProperty nodeProperty(NodeLabel label, String propertyKey);

    Set<RelationshipType> relationshipTypes();

    boolean hasRelationshipType(RelationshipType relationshipType);

    long relationshipCount();

    long relationshipCount(RelationshipType relationshipType);

    boolean hasRelationshipProperty(Collection<RelationshipType> relTypes, String propertyKey);

    NumberType relationshipPropertyType(String propertyKey);

    long relationshipPropertyCount();

    Set<String> relationshipPropertyKeys();

    Set<String> relationshipPropertyKeys(RelationshipType relationshipType);

    void addRelationshipType(
        RelationshipType relationshipType,
        Optional<String> relationshipPropertyKey,
        Optional<NumberType> relationshipPropertyType,
        HugeGraph.Relationships relationships
    );

    DeletionResult deleteRelationships(RelationshipType relationshipType);

    Graph getGraph(RelationshipType... relationshipTypes);

    Graph getGraph(RelationshipType relationshipType, Optional<String> relationshipProperty);

    Graph getGraph(Collection<RelationshipType> relationshipTypes, Optional<String> maybeRelationshipProperty);

    Graph getGraph(
        Collection<NodeLabel> nodeLabels,
        Collection<RelationshipType> relationshipTypes,
        Optional<String> maybeRelationshipProperty,
        int concurrency
    );

    Graph getUnion();

    void canRelease(boolean canRelease);

    void release();

    long nodeCount();

    GraphStoreSchema schema();

    @ValueClass
    interface NodeProperty {

        String key();

        NumberType type();

        CSRGraphStore.PropertyState state();

        NodeProperties values();

        static NodeProperty of(String key, NumberType type, CSRGraphStore.PropertyState origin, NodeProperties values) {
            return ImmutableNodeProperty.of(key, type, origin, values);
        }
    }
}
