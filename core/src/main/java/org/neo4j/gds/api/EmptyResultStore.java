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

public class EmptyResultStore implements ResultStore {
    @Override
    public void addNodePropertyValues(List<String> nodeLabels, String propertyKey, NodePropertyValues propertyValues) {

    }

    @Override
    public NodePropertyValues getNodePropertyValues(List<String> nodeLabels, String propertyKey) {
        return null;
    }

    @Override
    public void removeNodePropertyValues(List<String> nodeLabels, String propertyKey) {

    }

    @Override
    public void addNodeLabel(String nodeLabel, long nodeCount, LongUnaryOperator toOriginalId) {

    }

    @Override
    public boolean hasNodeLabel(String nodeLabel) {
        return false;
    }

    @Override
    public NodeLabelEntry getNodeIdsByLabel(String nodeLabel) {
        return null;
    }

    @Override
    public void removeNodeLabel(String nodeLabel) {

    }

    @Override
    public void addRelationship(String relationshipType, Graph graph, LongUnaryOperator toOriginalId) {

    }

    @Override
    public void addRelationship(
        String relationshipType,
        String propertyKey,
        Graph graph,
        LongUnaryOperator toOriginalId
    ) {

    }

    @Override
    public RelationshipEntry getRelationship(String relationshipType) {
        return null;
    }

    @Override
    public RelationshipEntry getRelationship(String relationshipType, String propertyKey) {
        return null;
    }

    @Override
    public void removeRelationship(String relationshipType) {

    }

    @Override
    public void removeRelationship(String relationshipType, String propertyKey) {

    }

    @Override
    public void addRelationshipStream(
        String relationshipType,
        List<String> propertyKeys,
        List<ValueType> propertyTypes,
        Stream<ExportedRelationship> relationshipStream,
        LongUnaryOperator toOriginalId
    ) {

    }

    @Override
    public RelationshipStreamEntry getRelationshipStream(String relationshipType, List<String> propertyKeys) {
        return null;
    }

    @Override
    public void removeRelationshipStream(String relationshipType, List<String> propertyKeys) {

    }

    @Override
    public void addRelationshipIterator(
        String relationshipType,
        List<String> propertyKeys,
        CompositeRelationshipIterator relationshipIterator,
        LongUnaryOperator toOriginalId
    ) {
        
    }

    @Override
    public RelationshipIteratorEntry getRelationshipIterator(String relationshipType, List<String> propertyKeys) {
        return null;
    }

    @Override
    public void removeRelationshipIterator(String relationshipType, List<String> propertyKeys) {

    }

    @Override
    public boolean hasRelationshipIterator(String relationshipType, List<String> propertyKeys) {
        return false;
    }

    @Override
    public boolean hasRelationship(String relationshipType) {
        return false;
    }

    @Override
    public boolean hasRelationship(String relationshipType, List<String> propertyKeys) {
        return false;
    }

    @Override
    public boolean hasRelationshipStream(String relationshipType, List<String> propertyKeys) {
        return false;
    }
}
