/*
 * Copyright (c) 2017-2019 "Neo4j,"
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

package org.neo4j.graphalgo.equality;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightMapping;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.loading.IdMap;
import org.neo4j.graphalgo.core.loading.NodePropertiesBuilder;
import org.neo4j.graphalgo.core.loading.NullWeightMap;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Element;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public final class TestGraph implements Graph {

    private final Map<Long, Adjacency> adjacencyList;
    private final Map<String, WeightMapping> nodeProperties;
    private final WeightMapping relationshipProperty;
    private final boolean hasRelationshipProperty;

    private TestGraph(
            Map<Long, Adjacency> adjacencyList,
            Map<String, WeightMapping> nodeProperties,
            WeightMapping relationshipProperty,
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
                .mapToLong(adjacency -> adjacency.outEdges.size())
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
    public Direction getLoadDirection() {
        return Direction.BOTH;
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
    public int degree(long nodeId, Direction direction) {
        return adjacencyList.get(nodeId).degree(direction);
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
    public WeightMapping nodeProperties(String type) {
        return nodeProperties.get(type);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return nodeProperties.keySet();
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        Adjacency adjacency = adjacencyList.get(nodeId);
        if (direction == Direction.BOTH) {
            forEach(adjacency.outEdges, consumer);
            forEach(adjacency.inEdges, consumer);
        } else if (direction == Direction.OUTGOING) {
            forEach(adjacency.outEdges, consumer);
        } else {
            forEach(adjacency.inEdges, consumer);
        }
    }

    private void forEach(List<Edge> edges, RelationshipConsumer consumer) {
        edges.forEach(e -> consumer.accept(e.getSourceVertexId(), e.getTargetVertexId()));
    }

    @Override
    public void forEachRelationship(
            long nodeId,
            Direction direction,
            double fallbackValue,
            WeightedRelationshipConsumer consumer) {
        Adjacency adjacency = adjacencyList.get(nodeId);
        if (direction == Direction.BOTH) {
            forEachWeighted(adjacency.outEdges, fallbackValue, consumer);
            forEachWeighted(adjacency.inEdges, fallbackValue, consumer);
        } else if (direction == Direction.OUTGOING) {
            forEachWeighted(adjacency.outEdges, fallbackValue, consumer);
        } else {
            forEachWeighted(adjacency.inEdges, fallbackValue, consumer);
        }
    }

    private void forEachWeighted(List<Edge> edges, double fallbackValue, WeightedRelationshipConsumer consumer) {
        if (!hasRelationshipProperty()) {
            forEach(edges, (s, t) -> consumer.accept(s, t, fallbackValue));
        } else {
            edges.forEach(e -> consumer.accept(
                    e.getSourceVertexId(),
                    e.getTargetVertexId(),
                    relationshipProperty.weight(e.getId())));
        }
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        ExistsConsumer consumer = new ExistsConsumer(targetNodeId);
        forEachRelationship(sourceNodeId, direction, consumer);
        return consumer.found;

    }

    @Override
    public double weightOf(long sourceNodeId, long targetNodeId, double fallbackValue) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RelationshipIntersect intersection() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getTarget(long nodeId, long index, Direction direction) {
        throw new UnsupportedOperationException();
    }

    private static class Adjacency {
        private final Vertex vertex;
        private final List<Edge> outEdges;
        private final List<Edge> inEdges;

        Adjacency(Vertex vertex, List<Edge> outEdges, List<Edge> inEdges) {
            this.vertex = vertex;
            this.outEdges = outEdges;
            this.inEdges = inEdges;
        }

        int degree(Direction direction) {
            int degree;
            if (direction == Direction.BOTH) {
                degree = outEdges.size() + inEdges.size();
            } else if (direction == Direction.OUTGOING) {
                degree = outEdges.size();
            } else {
                degree = inEdges.size();
            }
            return degree;
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
            return fromGdl(gdl, Optional.empty());
        }

        public static Graph fromGdl(String gdl, Optional<String> weightProperty) {
            Objects.requireNonNull(gdl);
            GDLHandler gdlHandler = new GDLHandler.Builder().buildFromString(gdl);
            Collection<Vertex> vertices = gdlHandler.getVertices();
            Collection<Edge> edges = gdlHandler.getEdges();

            validateInput(vertices, edges);

            Map<Long, Adjacency> adjacencyList = buildAdjacencyList(vertices, edges);
            Map<String, WeightMapping> nodeProperties = buildNodeProperties(vertices);
            WeightMapping relationshipProperty = buildRelationshipProperty(weightProperty, edges);

            return new TestGraph(adjacencyList, nodeProperties, relationshipProperty, weightProperty.isPresent());
        }

        private static void validateInput(Collection<Vertex> vertices, Collection<Edge> edges) {
            if (vertices.isEmpty()) {
                throw new IllegalArgumentException("Graph cannot be empty");
            }
            if (!hasConsecutiveIdSpace(vertices)) {
                throw new IllegalArgumentException("GdlGraph requires a consecutive node id space.");
            }
            if (!hasConsecutiveIdSpace(edges)) {
                throw new IllegalArgumentException("GdlGraph requires a consecutive edge id space.");
            }
        }

        private static boolean hasConsecutiveIdSpace(Collection<? extends Element> elements) {
            long maxVertexId = elements.parallelStream().mapToLong(Element::getId).max().orElse(-1);
            return (maxVertexId == elements.size() - 1);
        }

        private static Map<Long, Adjacency> buildAdjacencyList(
                Collection<Vertex> vertices,
                Collection<Edge> edges) {
            Map<Long, Adjacency> adjacencyList = new HashMap<>();

            for (Vertex vertex : vertices) {
                List<Edge> outEdges = edges.stream()
                        .filter(e -> e.getSourceVertexId() == vertex.getId())
                        .collect(toList());

                List<Edge> inEdges = edges.stream()
                        .filter(e -> e.getTargetVertexId() == vertex.getId())
                        .peek(e -> {
                            long tmp = e.getSourceVertexId();
                            e.setSourceVertexId(e.getTargetVertexId());
                            e.setTargetVertexId(tmp);
                        })
                        .collect(toList());

                adjacencyList.put(vertex.getId(), new Adjacency(vertex, outEdges, inEdges));
            }
            return adjacencyList;

        }

        private static Map<String, WeightMapping> buildNodeProperties(Collection<Vertex> vertices) {
            Vertex v = vertices.stream().findFirst().get();
            Map<String, NodePropertiesBuilder> nodePropertiesBuilders = v.getProperties()
                    .keySet()
                    .stream()
                    .collect(Collectors.toMap(
                            Function.identity(),
                            key -> NodePropertiesBuilder.of(vertices.size(), AllocationTracker.EMPTY, 1.0, -2, key)));

            vertices.forEach(vertex -> vertex.getProperties().forEach((key, value) -> {
                if (value instanceof Number) {
                    nodePropertiesBuilders.computeIfPresent(key, (unused, map) -> {
                        map.set(vertex.getId(), ((Number) value).doubleValue());
                        return map;
                    });
                } else {
                    throw new IllegalArgumentException("Node property value must be of type Number, but was " + value.getClass());
                }
            }));

            return nodePropertiesBuilders
                    .entrySet()
                    .stream()
                    .collect(toMap(Map.Entry::getKey, entry -> entry.getValue().build()));
        }

        private static WeightMapping buildRelationshipProperty(
                Optional<String> weightProperty,
                Collection<Edge> edges) {
            WeightMapping relationshipProperty = new NullWeightMap(1.0);
            if (weightProperty.isPresent()) {
                String propertyKey = weightProperty.get();
                NodePropertiesBuilder edgePropertiesBuilder = NodePropertiesBuilder.of(
                        edges.size(),
                        AllocationTracker.EMPTY,
                        1.0,
                        -2,
                        propertyKey);
                edges.forEach(edge -> {
                    if (!edge.getProperties().containsKey(propertyKey)) {
                        throw new IllegalArgumentException("Missing weight property for edge: " + edge);
                    } else {
                        Object value = edge.getProperties().get(propertyKey);
                        if (value instanceof Number) {
                            edgePropertiesBuilder.set(edge.getId(), ((Number) value).doubleValue());
                        } else {
                            throw new IllegalArgumentException("Relationship property value must be of type Number, but was " + value.getClass());
                        }
                    }
                });
                relationshipProperty = edgePropertiesBuilder.build();
            }
            return relationshipProperty;
        }
    }
}
