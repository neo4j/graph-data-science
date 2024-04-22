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

import java.util.List;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

/**
 * A store for write results that are not immediately persisted in the database.
 * This is mainly used for the session architecture, where algorithms results are first
 * written into this store and then streamed via arrow to persist them in a
 * remote database.
 */
public interface ResultStore {

    /**
     * Stores node property values under the given property key.
     */
    void addNodePropertyValues(List<String> nodeLabels, String propertyKey, NodePropertyValues propertyValues);

    /**
     * Retrieves node property values from this store based on the property key.
     */
    NodePropertyValues getNodePropertyValues(List<String> nodeLabels, String propertyKey);

    /**
     * Removes node property values from this store based on the property key.
     */
    void removeNodePropertyValues(List<String> nodeLabels, String propertyKey);

    /**
     * Stores node id information for the given label in this store.
     */
    void addNodeLabel(String nodeLabel, long nodeCount, LongUnaryOperator toOriginalId);

    /**
     * Checks if this store has node id information for the given label.
     */
    boolean hasNodeLabel(String nodeLabel);

    /**
     * Retrieves node id information for the given label.
     */
    NodeLabelEntry getNodeIdsByLabel(String nodeLabel);

    /**
     * Removes node id information for the given label from this store.
     */
    void removeNodeLabel(String nodeLabel);

    /**
     * Stores a relationship information in this store, held by the given graph.
     *
     * @param toOriginalId a mapping function to for the relationship source and target ids
     */
    void addRelationship(String relationshipType, Graph graph, LongUnaryOperator toOriginalId);

    /**
     * Stores a relationship information in this store, held by the given graph.
     *
     * @param propertyKey the property key for the relationship
     * @param toOriginalId a mapping function to for the relationship source and target ids
     */
    void addRelationship(String relationshipType, String propertyKey, Graph graph, LongUnaryOperator toOriginalId);

    /**
     * Retrieves relationship information from this store based on the relationship type.
     */
    RelationshipEntry getRelationship(String relationshipType);

    /**
     * Retrieves relationship information from this store based on the relationship type and property key.
     */
    RelationshipEntry getRelationship(String relationshipType, String propertyKey);

    /**
     * Removes relationship information from this store based on the relationship type.
     */
    void removeRelationship(String relationshipType);

    /**
     * Removes relationship information from this store based on the relationship type and property key.
     */
    void removeRelationship(String relationshipType, String propertyKey);

    /**
     * Stores a stream of relationships in this store.
     *
     * @param propertyKeys the property keys for the relationships
     * @param propertyTypes the property types for the relationship properties, expected to match the order of the property keys
     * @param toOriginalId a mapping function to for the relationship source and target ids
     */
    void addRelationshipStream(
        String relationshipType,
        List<String> propertyKeys,
        List<ValueType> propertyTypes,
        Stream<ExportedRelationship> relationshipStream,
        LongUnaryOperator toOriginalId
    );

    /**
     * Retrieves a stream of relationships from this store based on the relationship type and property keys.
     */
    RelationshipStreamEntry getRelationshipStream(String relationshipType, List<String> propertyKeys);

    /**
     * Removes a stream of relationships from this store based on the relationship type and property keys.
     */
    void removeRelationshipStream(String relationshipType, List<String> propertyKeys);

    /**
     * Checks if this store has a relationship of the given type.
     * Does not include relationship streams.
     */
    boolean hasRelationship(String relationshipType);

    /**
     * Checks if this store has a relationship of the given type and property keys.
     * Does not include relationship streams.
     */
    boolean hasRelationship(String relationshipType, List<String> propertyKeys);

    /**
     * Checks if this store has a relationship stream of the given type and properties.
     * Does not include non-stream relationships.
     */
    boolean hasRelationshipStream(String relationshipType, List<String> propertyKeys);

    record NodeLabelEntry(long nodeCount, LongUnaryOperator toOriginalId) {}

    record RelationshipEntry(Graph graph, LongUnaryOperator toOriginalId) {}

    record RelationshipStreamEntry(Stream<ExportedRelationship> relationshipStream, List<ValueType> propertyTypes, LongUnaryOperator toOriginalId) {}

    ResultStore EMPTY = new EmptyResultStore();
}
