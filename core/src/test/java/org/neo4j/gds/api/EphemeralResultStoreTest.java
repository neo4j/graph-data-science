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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;

import java.util.List;
import java.util.PrimitiveIterator;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class EphemeralResultStoreTest {

    @Test
    void shouldStoreNodeProperty() {
        var resultStore = new EphemeralResultStore();

        var propertyValues = mock(NodePropertyValues.class);
        resultStore.addNodePropertyValues("foo", propertyValues);

        assertThat(resultStore.getNodePropertyValues("foo")).isEqualTo(propertyValues);
    }

    @Test
    void shouldStoreNodeLabel() {
        var resultStore = new EphemeralResultStore();

        var nodeIds = mock(PrimitiveIterator.OfLong.class);
        resultStore.addNodeLabel("Label", nodeIds);

        assertThat(resultStore.getNodeIdsByLabel("Label")).isEqualTo(nodeIds);
    }

    @Test
    void shouldStoreGraphBasedRelationshipsWithoutProperty() {
        var resultStore = new EphemeralResultStore();

        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationship("Type", graph, toOriginalId);

        var relationshipEntry = resultStore.getRelationship("Type");
        assertThat(relationshipEntry.graph()).isEqualTo(graph);
        assertThat(relationshipEntry.toOriginalId()).isEqualTo(toOriginalId);

        assertThat(resultStore.hasRelationship("Type")).isTrue();
    }

    @Test
    void shouldStoreGraphBasedRelationshipsWithProperty() {
        var resultStore = new EphemeralResultStore();

        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationship("Type", "prop", graph, toOriginalId);

        var relationshipEntry = resultStore.getRelationship("Type", "prop");
        assertThat(relationshipEntry.graph()).isEqualTo(graph);
        assertThat(relationshipEntry.toOriginalId()).isEqualTo(toOriginalId);
    }

    @Test
    void shouldStoreStreamBasedRelationships() {
        var resultStore = new EphemeralResultStore();

        var relationshipStream = mock(Stream.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationshipStream("Type", List.of("foo"), List.of(ValueType.DOUBLE), relationshipStream, toOriginalId);

        var relationshipStreamEntry = resultStore. getRelationshipStream("Type", List.of("foo"));
        assertThat(relationshipStreamEntry.relationshipStream()).isEqualTo(relationshipStream);
        assertThat(relationshipStreamEntry.toOriginalId()).isEqualTo(toOriginalId);
    }
}
