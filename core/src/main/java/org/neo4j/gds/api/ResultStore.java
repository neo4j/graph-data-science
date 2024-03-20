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
import java.util.PrimitiveIterator;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

public interface ResultStore {

    void addNodeProperty(String propertyKey, NodePropertyValues propertyValues);

    NodePropertyValues getNodePropertyValues(String propertyKey);

    void addNodeLabel(String nodeLabel, PrimitiveIterator.OfLong nodeIds);

    PrimitiveIterator.OfLong getNodeIdsByLabel(String nodeLabel);

    void addRelationship(String relationshipType, Graph graph, LongUnaryOperator toOriginalId);

    void addRelationship(String relationshipType, String propertyKey, Graph graph, LongUnaryOperator toOriginalId);

    void addRelationshipStream(
        String relationshipType,
        List<String> propertyKeys,
        List<ValueType> propertyTypes,
        Stream<ExportedRelationship> relationshipStream,
        LongUnaryOperator toOriginalId
    );

    RelationshipStreamEntry getRelationshipStream(String relationshipType, List<String> propertyKeys);

    RelationshipEntry getRelationship(String relationshipType);

    RelationshipEntry getRelationship(String relationshipType, String propertyKey);

    boolean hasRelationship(String relationshipType);

    record RelationshipEntry(Graph graph, LongUnaryOperator toOriginalId) {}

    record RelationshipStreamEntry(Stream<ExportedRelationship> relationshipStream, List<ValueType> propertyTypes, LongUnaryOperator toOriginalId) {}
}
