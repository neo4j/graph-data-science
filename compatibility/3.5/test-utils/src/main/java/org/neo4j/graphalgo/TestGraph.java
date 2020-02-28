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
package org.neo4j.graphalgo;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.NodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.NullPropertyMap;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Element;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class TestGraph implements Graph {

    private static final int STREAM_CONCURRENCY = 4;

    private final Map<Long, Adjacency> adjacencyList;
    private final Map<String, NodeProperties> nodeProperties;
    private final NodeProperties relationshipProperty;
    private final boolean hasRelationshipProperty;

    private TestGraph(
            Map<Long, Adjacency> adjacencyList,
            Map<String, NodeProperties> nodeProperties,
            NodeProperties relationshipProperty,
            boolean hasRelationshipProperty) {
        this.adjacencyList = adjacencyList;
        this.nodeProperties = nodeProperties;
        this.relationshipProperty = relationshipProperty;
        this.hasRelationshipProperty = hasRelationshipProperty;
    }

    @Override
    public long nodeCount() {
        return adjacencyList.size();
    }

    @Override
    public long relationshipCount() {
        return adjacencyList.values()
                .parallelStream()
                .mapToLong(adjacency -> adjacency.edges.size())
                .sum();
    }

    @Override
    public boolean isUndirected() {
        return false;
    }

    @Override
    public boolean hasRelationshipProperty() {
        return hasRelationshipProperty;
    }

    @Override
    public void canRelease(boolean canRelease) { }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        return LazyBatchCollection.of(
                nodeCount(),
                batchSize,
                (start, length) -> () -> PrimitiveLongCollections.range(start, start + length - 1L));
    }

    @Override
    public int degree(long nodeId) {
        return adjacencyList.get(nodeId).degree();
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return nodeId;
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return nodeId;
    }

    @Override
    public boolean contains(long nodeId) {
        return adjacencyList.containsKey(nodeId);
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        final long count = nodeCount();
        for (long i = 0L; i < count; i++) {
            if (!consumer.test(i)) {
                return;
            }
        }
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return new IdMap.IdIterator(nodeCount());
    }

    @Override
    public NodeProperties nodeProperties(String type) {
        return nodeProperties.get(type);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return nodeProperties.keySet();
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        Adjacency adjacency = adjacencyList.get(nodeId);
        forEach(adjacency.edges, consumer);
    }

    private void forEach(List<Relationship> rels, RelationshipConsumer consumer) {
        rels.forEach(r -> consumer.accept(r.sourceId, r.targetId));
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        Adjacency adjacency = adjacencyList.get(nodeId);
        forEachWithProperty(adjacency.edges, fallbackValue, consumer);
    }

    private void forEachWithProperty(List<Relationship> rels, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        if (!hasRelationshipProperty()) {
            forEach(rels, (s, t) -> consumer.accept(s, t, fallbackValue));
        } else {
            rels.forEach(r -> consumer.accept(
                    r.sourceId,
                    r.targetId,
                    relationshipProperty.nodeProperty(r.id)));
        }
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        ExistsConsumer consumer = new ExistsConsumer(targetNodeId);
        forEachRelationship(sourceNodeId, consumer);
        return consumer.found;

    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        throw new UnsupportedOperationException("org.neo4j.graphalgo.TestGraph.relationshipProperty is not implemented.");
    }

    @Override
    public RelationshipIntersect intersection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTarget(long nodeId, long index) {
        throw new UnsupportedOperationException();
    }

    private static class Relationship {
        private final long id;
        private final long sourceId;
        private final long targetId;

        Relationship(long id, long sourceId, long targetId) {
            this.id = id;
            this.sourceId = sourceId;
            this.targetId = targetId;
        }

        Relationship flip() {
            return new Relationship(id, targetId, sourceId);
        }
    }

    private static class Adjacency {
        private final List<Relationship> edges;

        Adjacency(List<Relationship> edges) {
            this.edges = edges;
        }

        int degree() {
            return edges.size();
        }
    }

    private static class ExistsConsumer implements RelationshipConsumer {
        private final long targetNodeId;
        private boolean found = false;

        ExistsConsumer(long targetNodeId) {
            this.targetNodeId = targetNodeId;
        }

        @Override
        public boolean accept(long s, long t) {
            if (t == targetNodeId) {
                found = true;
                return false;
            }
            return true;
        }
    }

    public static final class Builder {

        private Builder() {}

        public static Graph fromGdl(String gdl) {
            return fromGdl(gdl, Orientation.NATURAL);
        }

        public static Graph fromGdl(String gdl, Orientation orientation) {
            Objects.requireNonNull(gdl);

            GDLHandler gdlHandler = new GDLHandler.Builder().buildFromString(gdl);
            Collection<Vertex> vertices = gdlHandler.getVertices();
            Collection<Edge> edges = gdlHandler.getEdges();

            validateInput(vertices, edges);

            Map<Long, Adjacency> adjacencyList = buildAdjacencyList(vertices, edges, orientation);
            Map<String, NodeProperties> nodeProperties = buildWeightMappings(vertices);
            Map<String, NodeProperties> relationshipProperties = buildWeightMappings(edges);

            // required because of single rel property limitation
            if (relationshipProperties.size() > 1) {
                throw new IllegalArgumentException("Graph supports at most one relationship property.");
            }
            boolean hasRelationshipProperty = !relationshipProperties.isEmpty();
            NodeProperties relationshipProperty = relationshipProperties
                    .values()
                    .stream()
                    .findFirst()
                    .orElseGet(() -> new NullPropertyMap(1.0));

            return new TestGraph(adjacencyList, nodeProperties, relationshipProperty, hasRelationshipProperty);
        }

        private static void validateInput(Collection<Vertex> vertices, Collection<Edge> edges) {
            if (!haveSameProperties(vertices)) {
                throw new IllegalArgumentException("Vertices must have the same set of property keys.");
            }
            if (!haveSameProperties(edges)) {
                throw new IllegalArgumentException("Relationships must have the same set of property keys.");
            }
        }

        private static boolean haveSameProperties(Collection<? extends Element> elements) {
            if (elements.isEmpty()) {
                return true;
            }
            Set<String> head = elements.stream().findFirst().get().getProperties().keySet();
            return elements
                    .parallelStream()
                    .map(Element::getProperties)
                    .map(Map::keySet)
                    .allMatch(keys -> keys.equals(head));
        }

        private static Map<Long, Adjacency> buildAdjacencyList(
            Collection<Vertex> vertices,
            Collection<Edge> edges,
            Orientation orientation
        ) {
            Map<Long, Adjacency> adjacencyList = new HashMap<>();

            for (Vertex vertex : vertices) {
                List<Relationship> outgoing = edges.stream()
                    .filter(e -> e.getSourceVertexId() == vertex.getId())
                    .map(e -> new Relationship(e.getId(), e.getSourceVertexId(), e.getTargetVertexId()))
                    .collect(toList());

                List<Relationship> incoming = edges.stream()
                    .filter(e -> e.getTargetVertexId() == vertex.getId())
                    .map(e -> new Relationship(e.getId(), e.getTargetVertexId(), e.getSourceVertexId()))
                    .collect(toList());

                if (orientation == Orientation.UNDIRECTED) {
                    outgoing.addAll(incoming);
                }

                adjacencyList.put(vertex.getId(), new Adjacency(orientation == Orientation.REVERSE ? incoming : outgoing));
            }

            return adjacencyList;
        }

        private static <T extends Element> Map<String, NodeProperties> buildWeightMappings(Collection<T> elements) {

            if (elements.isEmpty()) {
                return new HashMap<>(0);
            }

            Map<String, Object> properties = elements.stream().findFirst().get().getProperties();

            if (properties.isEmpty()) {
                return new HashMap<>(0);
            }

            Map<String, NodePropertiesBuilder> nodePropertiesBuilders = properties
                .keySet()
                .parallelStream()
                .collect(Collectors.toMap(
                    Function.identity(),
                    key -> NodePropertiesBuilder.of(
                        elements.size(),
                        AllocationTracker.EMPTY,
                        1.0,
                        -2,
                        key,
                        STREAM_CONCURRENCY
                    )
                ));

            elements.forEach(element ->
                    element.getProperties().forEach((propertyKey, value) ->
                            nodePropertiesBuilders.compute(propertyKey, storeValue(element, value))));

            return nodePropertiesBuilders
                    .entrySet()
                    .stream()
                    .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().build()));
        }

        private static BiFunction<String, NodePropertiesBuilder, NodePropertiesBuilder> storeValue(
                final Element element,
                final Object value) {
            return (propertyKey, builder) -> {
                if (value instanceof Number) {
                    builder.set(element.getId(), ((Number) value).doubleValue());
                    return builder;
                } else {
                    throw new IllegalArgumentException(String.format(
                            "%s property '%s' of must be of type Number, but was %s for %s.",
                            element.getClass().getSimpleName(),
                            propertyKey,
                            value.getClass(),
                            element));
                }
            };
        }
    }
}
