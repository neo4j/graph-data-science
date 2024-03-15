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

import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

public class EphemeralResultStore implements ResultStore {

    private static final String NO_PROPERTY_KEY = "";

    private final Map<String, NodePropertyValues> nodeProperties;
    private final Map<String, PrimitiveIterator.OfLong> nodeIdsByLabel;
    private final Map<RelationshipKey, RelationshipEntry> relationships;
    private final Map<RelationshipKey, RelationshipStreamEntry> relationshipStreams;

    public EphemeralResultStore() {
        this.nodeProperties = new HashMap<>();
        this.nodeIdsByLabel = new HashMap<>();
        this.relationships = new HashMap<>();
        this.relationshipStreams = new HashMap<>();
    }

    @Override
    public void addNodeProperty(String propertyKey, NodePropertyValues propertyValues) {
        this.nodeProperties.put(propertyKey, propertyValues);
    }

    @Override
    public void removeNodeProperty(String propertyKey) {
        this.nodeProperties.remove(propertyKey);
    }

    @Override
    public NodePropertyValues getNodePropertyValues(String propertyKey) {
        return this.nodeProperties.get(propertyKey);
    }

    @Override
    public void addNodeLabel(String nodeLabel, PrimitiveIterator.OfLong nodeIds) {
        this.nodeIdsByLabel.put(nodeLabel, nodeIds);
    }

    @Override
    public void removeNodeLabel(String nodeLabel) {
        this.nodeIdsByLabel.remove(nodeLabel);
    }

    @Override
    public PrimitiveIterator.OfLong getNodeIdsByLabel(String nodeLabel) {
        return this.nodeIdsByLabel.get(nodeLabel);
    }

    @Override
    public void addRelationship(String relationshipType, Graph graph, LongUnaryOperator toOriginalId) {
        addRelationship(relationshipType, NO_PROPERTY_KEY, graph, toOriginalId);
    }

    @Override
    public void addRelationship(
        String relationshipType,
        List<String> propertyKeys,
        Graph graph,
        LongUnaryOperator toOriginalId
    ) {
        this.relationships.put(new RelationshipKey(relationshipType, propertyKeys), new RelationshipEntry(graph, toOriginalId));
    }

    @Override
    public void removeRelationship(String relationshipType) {
        removeRelationship(relationshipType, NO_PROPERTY_KEY);
    }

    @Override
    public void removeRelationship(String relationshipType, List<String> propertyKey) {
        this.relationships.remove(new RelationshipKey(relationshipType, propertyKey));
    }

    @Override
    public RelationshipEntry getRelationships(String relationshipType) {
        return getRelationships(relationshipType, NO_PROPERTY_KEY);
    }

    @Override
    public RelationshipEntry getRelationships(String relationshipType, List<String> propertyKey) {
        return this.relationships.get(new RelationshipKey(relationshipType, propertyKey));
    }

    @Override
    public void addRelationshipStream(
        String relationshipType,
        List<String> propertyKeys,
        List<ValueType> propertyTypes,
        Stream<ExportedRelationship> relationshipStream,
        LongUnaryOperator toOriginalId
    ) {
        this.relationshipStreams.put(
            new RelationshipKey(relationshipType, propertyKeys),
            new RelationshipStreamEntry(relationshipStream, propertyTypes, toOriginalId)
        );
    }

    @Override
    public void removeRelationshipStream(String relationshipType, List<String> propertyKeys) {
        this.relationshipStreams.remove(new RelationshipKey(relationshipType, propertyKeys));
    }

    @Override
    public RelationshipStreamEntry getRelationshipStream(String relationshipType, List<String> propertyKeys) {
        return this.relationshipStreams.get(new RelationshipKey(relationshipType, propertyKeys));
    }

    private record RelationshipKey(String relationshipType, List<String> propertyKeys) {}
}
