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
import org.neo4j.gds.extension.FakeClockExtension;
import org.neo4j.gds.extension.Inject;
import org.neo4j.time.FakeClock;

import java.util.List;
import java.util.function.LongUnaryOperator;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@FakeClockExtension
class EphemeralResultStoreTest {

    @Inject
    FakeClock clock;

    @Test
    void shouldStoreNodeProperty() {
        var resultStore = new EphemeralResultStore();

        var propertyValues = mock(NodePropertyValues.class);
        resultStore.addNodePropertyValues(List.of("A", "B"), "foo", propertyValues);

        assertThat(resultStore.getNodePropertyValues(List.of("A", "B"), "foo")).isEqualTo(propertyValues);
    }

    @Test
    void shouldNotResolveNodePropertiesWhenLabelsDoNotMatch() {
        var resultStore = new EphemeralResultStore();

        var propertyValues = mock(NodePropertyValues.class);
        resultStore.addNodePropertyValues(List.of("A"), "foo", propertyValues);

        assertThat(resultStore.getNodePropertyValues(List.of("B"), "foo")).isNull();
    }

    @Test
    void shouldRemoveNodeProperty() {
        var resultStore = new EphemeralResultStore();

        var propertyValues = mock(NodePropertyValues.class);
        resultStore.addNodePropertyValues(List.of("A", "B"), "foo", propertyValues);

        assertThat(resultStore.getNodePropertyValues(List.of("A", "B"), "foo")).isNotNull();

        resultStore.removeNodePropertyValues(List.of("A", "B"), "foo");

        assertThat(resultStore.getNodePropertyValues(List.of("A", "B"), "foo")).isNull();
    }

    @Test
    void shouldEvictNodePropertyEntryAfter10Minutes() throws InterruptedException {
        var resultStore = new EphemeralResultStore();

        var propertyValues = mock(NodePropertyValues.class);
        resultStore.addNodePropertyValues(List.of("A"), "prop", propertyValues);

        assertThat(resultStore.getNodePropertyValues(List.of("A"), "prop")).isNotNull();

        clock.forward(EphemeralResultStore.CACHE_EVICTION_DURATION.plusMinutes(1));
        // make some room for the cache eviction thread to trigger a cleanup
        Thread.sleep(100);

        assertThat(resultStore.getNodePropertyValues(List.of("A"), "prop")).isNull();
    }

    @Test
    void shouldStoreNodeLabel() {
        var resultStore = new EphemeralResultStore();

        var nodeCount = 1337L;
        var toOriginalId = mock(LongUnaryOperator.class);
        resultStore.addNodeLabel("Label", nodeCount, toOriginalId);

        var nodeLabelEntry = resultStore.getNodeIdsByLabel("Label");
        assertThat(nodeLabelEntry.nodeCount()).isEqualTo(1337L);
        assertThat(nodeLabelEntry.toOriginalId()).isEqualTo(toOriginalId);

        assertThat(resultStore.hasNodeLabel("Label")).isTrue();
    }

    @Test
    void shouldRemoveNodeLabel() {
        var resultStore = new EphemeralResultStore();

        var nodeCount = 1337L;
        var toOriginalId = mock(LongUnaryOperator.class);
        resultStore.addNodeLabel("Label", nodeCount, toOriginalId);

        assertThat(resultStore.hasNodeLabel("Label")).isTrue();

        resultStore.removeNodeLabel("Label");

        assertThat(resultStore.hasNodeLabel("Label")).isFalse();
    }

    @Test
    void shouldEvictNodeLabelEntryAfter10Minutes() throws InterruptedException {
        var resultStore = new EphemeralResultStore();

        var nodeCount = 1337L;
        var toOriginalId = mock(LongUnaryOperator.class);
        resultStore.addNodeLabel("Label", nodeCount, toOriginalId);

        assertThat(resultStore.hasNodeLabel("Label")).isTrue();

        clock.forward(EphemeralResultStore.CACHE_EVICTION_DURATION.plusMinutes(1));
        // make some room for the cache eviction thread to trigger a cleanup
        Thread.sleep(100);

        assertThat(resultStore.hasNodeLabel("Label")).isFalse();
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
    void shouldRemoveGraphBasedRelationshipsWithoutProperty() {
        var resultStore = new EphemeralResultStore();

        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationship("Type", graph, toOriginalId);

        assertThat(resultStore.hasRelationship("Type")).isTrue();

        resultStore.removeRelationship("Type");

        assertThat(resultStore.hasRelationship("Type")).isFalse();
    }

    @Test
    void shouldEvictGraphBasedRelationshipsWithoutPropertyAfter10Minutes() throws InterruptedException {
        var resultStore = new EphemeralResultStore();

        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationship("Type", graph, toOriginalId);

        assertThat(resultStore.hasRelationship("Type")).isTrue();

        clock.forward(EphemeralResultStore.CACHE_EVICTION_DURATION.plusMinutes(1));
        // make some room for the cache eviction thread to trigger a cleanup
        Thread.sleep(100);

        assertThat(resultStore.hasRelationship("Type")).isFalse();
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

        assertThat(resultStore.hasRelationship("Type", List.of("prop"))).isTrue();
    }

    @Test
    void shouldRemoveGraphBasedRelationshipsWithProperty() {
        var resultStore = new EphemeralResultStore();

        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationship("Type", "prop", graph, toOriginalId);

        assertThat(resultStore.hasRelationship("Type", List.of("prop"))).isTrue();

        resultStore.removeRelationship("Type", "prop");

        assertThat(resultStore.hasRelationship("Type", List.of("prop"))).isFalse();
    }

    @Test
    void shouldEvictGraphBasedRelationshipsWithPropertyAfter10Minutes() throws InterruptedException {
        var resultStore = new EphemeralResultStore();

        var graph = mock(Graph.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationship("Type", "prop", graph, toOriginalId);

        assertThat(resultStore.hasRelationship("Type", List.of("prop"))).isTrue();


        clock.forward(EphemeralResultStore.CACHE_EVICTION_DURATION.plusMinutes(1));
        // make some room for the cache eviction thread to trigger a cleanup
        Thread.sleep(100);

        assertThat(resultStore.hasRelationship("Type", List.of("prop"))).isFalse();
    }

    @Test
    void shouldStoreStreamBasedRelationships() {
        var resultStore = new EphemeralResultStore();

        var relationshipStream = mock(Stream.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationshipStream("Type", List.of("foo"), List.of(ValueType.DOUBLE), relationshipStream, toOriginalId);

        var relationshipStreamEntry = resultStore.getRelationshipStream("Type", List.of("foo"));
        assertThat(relationshipStreamEntry.relationshipStream()).isEqualTo(relationshipStream);
        assertThat(relationshipStreamEntry.toOriginalId()).isEqualTo(toOriginalId);

        assertThat(resultStore.hasRelationshipStream("Type", List.of("foo"))).isTrue();
        assertThat(resultStore.hasRelationshipStream("Type", List.of())).isFalse();
    }

    @Test
    void shouldRemoveStreamBasedRelationships() {
        var resultStore = new EphemeralResultStore();

        var relationshipStream = mock(Stream.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationshipStream("Type", List.of("foo"), List.of(ValueType.DOUBLE), relationshipStream, toOriginalId);

        assertThat(resultStore.hasRelationshipStream("Type", List.of("foo"))).isTrue();

        resultStore.removeRelationshipStream("Type", List.of("foo"));

        assertThat(resultStore.hasRelationshipStream("Type", List.of("foo"))).isFalse();
    }

    @Test
    void shouldEvictStreamBasedRelationshipsAfter10Minutes() throws InterruptedException {
        var resultStore = new EphemeralResultStore();

        var relationshipStream = mock(Stream.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationshipStream("Type", List.of("foo"), List.of(ValueType.DOUBLE), relationshipStream, toOriginalId);

        assertThat(resultStore.hasRelationshipStream("Type", List.of("foo"))).isTrue();

        clock.forward(EphemeralResultStore.CACHE_EVICTION_DURATION.plusMinutes(1));
        // make some room for the cache eviction thread to trigger a cleanup
        Thread.sleep(100);

        assertThat(resultStore.hasRelationshipStream("Type", List.of("foo"))).isFalse();
    }

    @Test
    void shouldStoreRelationshipIterator() {
        var resultStore = new EphemeralResultStore();

        var relationshipIterator = mock(CompositeRelationshipIterator.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationshipIterator("Type", List.of("foo", "bar"), relationshipIterator, toOriginalId);

        var relationshipIteratorEntry = resultStore.getRelationshipIterator("Type", List.of("foo", "bar"));
        assertThat(relationshipIteratorEntry.relationshipIterator()).isEqualTo(relationshipIterator);
        assertThat(relationshipIteratorEntry.toOriginalId()).isEqualTo(toOriginalId);
        assertThat(resultStore.hasRelationshipIterator("Type", List.of("foo", "bar"))).isTrue();
    }

    @Test
    void shouldRemoveRelationshipIterator() {
        var resultStore = new EphemeralResultStore();

        var relationshipIterator = mock(CompositeRelationshipIterator.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationshipIterator("Type", List.of("foo", "bar"), relationshipIterator, toOriginalId);

        assertThat(resultStore.hasRelationshipIterator("Type", List.of("foo", "bar"))).isTrue();

        resultStore.removeRelationshipIterator("Type", List.of("foo", "bar"));

        assertThat(resultStore.hasRelationshipIterator("Type", List.of("foo", "bar"))).isFalse();
    }

    @Test
    void shouldEvictIteratorBasedRelationshipsAfter10Minutes() throws InterruptedException {
        var resultStore = new EphemeralResultStore();

        var relationshipIterator = mock(CompositeRelationshipIterator.class);
        var toOriginalId = mock(LongUnaryOperator.class);

        resultStore.addRelationshipIterator("Type", List.of("foo"), relationshipIterator, toOriginalId);

        assertThat(resultStore.hasRelationshipIterator("Type", List.of("foo"))).isTrue();

        clock.forward(EphemeralResultStore.CACHE_EVICTION_DURATION.plusMinutes(1));
        // make some room for the cache eviction thread to trigger a cleanup
        Thread.sleep(100);

        assertThat(resultStore.hasRelationshipIterator("Type", List.of("foo"))).isFalse();
    }
}
