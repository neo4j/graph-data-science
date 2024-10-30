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

public interface ResultStoreEntry {

    <T> T accept(Visitor<T> visitor);

    interface Visitor<T> {
        T nodeLabel(String nodeLabel, long nodeCount, LongUnaryOperator toOriginalId);

        T nodeProperties(
            List<String> nodeLabels, List<String> propertyKeys, List<NodePropertyValues> propertyValues,
            LongUnaryOperator toOriginalId
        );

        T relationshipTopology(String relationshipType, Graph graph, LongUnaryOperator toOriginalId);

        T relationships(String relationshipType, String propertyKey, Graph graph, LongUnaryOperator toOriginalId);

        T relationshipStream(
            String relationshipType,
            List<String> propertyKeys,
            List<ValueType> propertyTypes,
            Stream<ExportedRelationship> relationshipStream,
            LongUnaryOperator toOriginalId
        );

        T relationshipIterators(
            String relationshipType,
            List<String> propertyKeys,
            CompositeRelationshipIterator relationshipIterator,
            LongUnaryOperator toOriginalId,
            long nodeCount
        );
    }

    record NodeLabel(String nodeLabel, long nodeCount, LongUnaryOperator toOriginalId) implements ResultStoreEntry {
        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.nodeLabel(nodeLabel, nodeCount, toOriginalId);
        }
    }

    record NodeProperties(
        List<String> nodeLabels,
        List<String> propertyKeys,
        List<NodePropertyValues> propertyValues,
        LongUnaryOperator toOriginalId
    ) implements ResultStoreEntry {
        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.nodeProperties(nodeLabels, propertyKeys, propertyValues, toOriginalId);
        }
    }

    record RelationshipTopology(String relationshipType, Graph graph, LongUnaryOperator toOriginalId) implements
        ResultStoreEntry {
        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.relationshipTopology(relationshipType, graph, toOriginalId);
        }
    }

    record RelationshipsFromGraph(
        String relationshipType,
        String propertyKey,
        Graph graph,
        LongUnaryOperator toOriginalId
    ) implements ResultStoreEntry {
        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.relationships(relationshipType, propertyKey, graph, toOriginalId);
        }
    }

    record RelationshipStream(
        String relationshipType,
        List<String> propertyKeys,
        List<ValueType> propertyTypes,
        Stream<ExportedRelationship> relationshipStream,
        LongUnaryOperator toOriginalId
    ) implements ResultStoreEntry {
        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.relationshipStream(
                relationshipType,
                propertyKeys,
                propertyTypes,
                relationshipStream,
                toOriginalId
            );
        }
    }

    record RelationshipIterators(
        String relationshipType,
        List<String> propertyKeys,
        CompositeRelationshipIterator relationshipIterator,
        LongUnaryOperator toOriginalId,
        long nodeCount
    ) implements ResultStoreEntry {
        @Override
        public <T> T accept(Visitor<T> visitor) {
            return visitor.relationshipIterators(
                relationshipType,
                propertyKeys,
                relationshipIterator,
                toOriginalId,
                nodeCount
            );
        }
    }
}
