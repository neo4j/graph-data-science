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
package org.neo4j.gds.core.huge;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.CSRGraph;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphCharacteristics;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.RelationshipConsumer;
import org.neo4j.gds.api.RelationshipCursor;
import org.neo4j.gds.api.RelationshipWithPropertyConsumer;
import org.neo4j.gds.api.Topology;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.core.utils.collection.primitive.PrimitiveLongIterable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PrimitiveIterator;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.core.huge.HugeGraph.NO_PROPERTY_VALUE;

public final class UnionGraph implements CSRGraph {

    private final CSRGraph first;
    private final List<? extends CSRGraph> graphs;
    private final Map<RelationshipType, Topology> relationshipTypeTopologies;

    public static CSRGraph of(List<? extends CSRGraph> graphs) {
        if (graphs.isEmpty()) {
            throw new IllegalArgumentException("no graphs");
        }
        if (graphs.size() == 1) {
            return graphs.get(0);
        }
        return new UnionGraph(graphs);
    }

    private UnionGraph(List<? extends CSRGraph> graphs) {
        first = graphs.get(0);
        this.graphs = graphs;
        this.relationshipTypeTopologies = new HashMap<>();
        graphs.forEach(graph -> relationshipTypeTopologies.putAll(graph.relationshipTopologies()));
    }

    @Override
    public long nodeCount() {
        return first.nodeCount();
    }

    @Override
    public long nodeCount(NodeLabel nodeLabel) {
        return first.nodeCount(nodeLabel);
    }

    @Override
    public OptionalLong rootNodeCount() {
        return first.rootNodeCount();
    }

    @Override
    public long highestOriginalId() {
        return first.highestOriginalId();
    }

    @Override
    public GraphSchema schema() {
        return graphs
            .stream()
            .map(Graph::schema)
            .reduce(GraphSchema::union)
            .orElseThrow(() -> new IllegalArgumentException("no graphs"));
    }

    @Override
    public GraphCharacteristics characteristics() {
        return graphs
            .stream()
            .map(Graph::characteristics)
            .reduce(GraphCharacteristics::intersect)
            .orElseThrow(() -> new IllegalArgumentException("no graphs"));
    }

    @Override
    public long relationshipCount() {
        return graphs.stream().mapToLong(Graph::relationshipCount).sum();
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(final long batchSize) {
        return first.batchIterables(batchSize);
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        first.forEachNode(consumer);
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator() {
        return first.nodeIterator();
    }

    @Override
    public PrimitiveIterator.OfLong nodeIterator(Set<NodeLabel> labels) {
        return first.nodeIterator(labels);
    }

    @Override
    public NodePropertyValues nodeProperties(final String propertyKey) {
        return first.nodeProperties(propertyKey);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return first.availableNodeProperties();
    }

    @Override
    public long toMappedNodeId(long originalNodeId) {
        return first.toMappedNodeId(originalNodeId);
    }

    @Override
    public long toOriginalNodeId(long mappedNodeId) {
        return first.toOriginalNodeId(mappedNodeId);
    }

    @Override
    public long toRootNodeId(long mappedNodeId) {
        return first.toRootNodeId(mappedNodeId);
    }

    @Override
    public IdMap rootIdMap() {
        return first.rootIdMap();
    }

    @Override
    public boolean contains(final long originalNodeId) {
        return first.contains(originalNodeId);
    }

    @Override
    public double relationshipProperty(final long sourceNodeId, final long targetNodeId, double fallbackValue) {
        for(Graph graph: graphs) {
            var property = graph.relationshipProperty(sourceNodeId, targetNodeId, fallbackValue);
            if (!(Double.isNaN(property) || property == fallbackValue)) {
                return property;
            }
        }

        return fallbackValue;
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        for(Graph graph: graphs) {
            var property = graph.relationshipProperty(sourceNodeId, targetNodeId);
            if (!Double.isNaN(property)) {
                return property;
            }
        }

        return NO_PROPERTY_VALUE;
    }

    @Override
    public Map<RelationshipType, Topology> relationshipTopologies() {
        return relationshipTypeTopologies;
    }

    @Override
    public void forEachRelationship(long nodeId, RelationshipConsumer consumer) {
        for (Graph graph : graphs) {
            graph.forEachRelationship(nodeId, consumer);
        }
    }

    @Override
    public void forEachRelationship(long nodeId, double fallbackValue, RelationshipWithPropertyConsumer consumer) {
        for (Graph graph : graphs) {
            graph.forEachRelationship(nodeId, fallbackValue, consumer);
        }
    }

    @Override
    public void forEachInverseRelationship(long nodeId, RelationshipConsumer consumer) {
        for (Graph graph : graphs) {
            graph.forEachInverseRelationship(nodeId, consumer);
        }
    }

    @Override
    public void forEachInverseRelationship(
        long nodeId,
        double fallbackValue,
        RelationshipWithPropertyConsumer consumer
    ) {
        for (Graph graph : graphs) {
            graph.forEachInverseRelationship(nodeId, fallbackValue, consumer);
        }
    }

    @Override
    public Stream<RelationshipCursor> streamRelationships(long nodeId, double fallbackValue) {
        return graphs
            .stream()
            .flatMap(graph -> graph.streamRelationships(nodeId, fallbackValue));
    }

    @Override
    public Graph relationshipTypeFilteredGraph(Set<RelationshipType> relationshipTypes) {
        List<CSRGraph> filteredGraphs = new ArrayList<>();
        for (CSRGraph graph : graphs) {
            if (relationshipTypes.isEmpty() || relationshipTypes.containsAll(graph.schema().relationshipSchema().availableTypes())) {
                filteredGraphs.add(graph);
            }
        }
        return UnionGraph.of(filteredGraphs);
    }

    @Override
    public int degree(long nodeId) {
        long degree = 0;

        for (CSRGraph graph : graphs) {
            degree += graph.degree(nodeId);
        }

        return Math.toIntExact(degree);
    }

    @Override
    public int degreeInverse(long nodeId) {
        long degree = 0;

        for (CSRGraph graph : graphs) {
            degree += graph.degreeInverse(nodeId);
        }

        return Math.toIntExact(degree);
    }

    @Override
    public int degreeWithoutParallelRelationships(long nodeId) {
        if (!isMultiGraph()) {
            return degree(nodeId);
        }
        var degreeCounter = new ParallelRelationshipDegreeCounter();
        graphs.forEach(graph -> graph.forEachRelationship(nodeId, degreeCounter));
        return degreeCounter.degree();
    }

    @Override
    public CSRGraph concurrentCopy() {
        return of(graphs.stream().map(CSRGraph::concurrentCopy).collect(Collectors.toList()));
    }

    @Override
    public Optional<NodeFilteredGraph> asNodeFilteredGraph() {
        return first.asNodeFilteredGraph();
    }

    /**
     * O(n) !
     */
    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        return graphs.stream().anyMatch(g -> g.exists(sourceNodeId, targetNodeId));
    }

    @Override
    public long nthTarget(long nodeId, int offset) {
        int remaining = offset;
        for (CSRGraph graph : graphs) {
            var localDegree = graph.degree(nodeId);
            if (localDegree > remaining) {
                return graph.nthTarget(nodeId, remaining);
            }
            remaining -= localDegree;
        }
        return NOT_FOUND;
    }

    @Override
    public void canRelease(boolean canRelease) {
        for (Graph graph : graphs) {
            graph.canRelease(canRelease);
        }
    }

    @Override
    public void releaseTopology() {
        for (Graph graph : graphs) {
            graph.releaseTopology();
        }
    }

    @Override
    public void releaseProperties() {
        for (Graph graph : graphs) {
            graph.releaseProperties();
        }
    }

    @Override
    public boolean hasRelationshipProperty() {
        return first.hasRelationshipProperty();
    }


    @Override
    public boolean isMultiGraph() {
        // we need to run a check across all relationships between the sub-graphs of the union
        // maybe we'll do that later; for now union never guarantees parallel-free
        return true;
    }

    public CompositeAdjacencyList relationshipTopology() {
        var adjacencies = graphs
            .stream()
            .map(CSRGraph::relationshipTopologies)
            .map(Map::values)
            .flatMap(Collection::stream)
            .map(Topology::adjacencyList)
            .collect(Collectors.toList());
        if (isNodeFilteredGraph()) {
            return CompositeAdjacencyList.withFilteredIdMap(adjacencies, first);
        }
        return CompositeAdjacencyList.of(adjacencies);
    }

    @Override
    public List<NodeLabel> nodeLabels(long mappedNodeId) {
        return first.nodeLabels(mappedNodeId);
    }

    @Override
    public void forEachNodeLabel(long mappedNodeId, NodeLabelConsumer consumer) {
        first.forEachNodeLabel(mappedNodeId, consumer);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return first.availableNodeLabels();
    }

    @Override
    public boolean hasLabel(long mappedNodeId, NodeLabel label) {
        return first.hasLabel(mappedNodeId, label);
    }

    public boolean isNodeFilteredGraph() {
        return first instanceof NodeFilteredGraph;
    }

    private static class ParallelRelationshipDegreeCounter implements RelationshipConsumer {
        private final BitSet visited;

        ParallelRelationshipDegreeCounter() {
            visited = BitSet.newInstance();
        }

        @Override
        public boolean accept(long s, long t) {
            visited.set(t);
            return true;
        }

        int degree() {
            return Math.toIntExact(visited.cardinality());
        }
    }

    public void addNodeLabel(NodeLabel nodeLabel) {
        throw new UnsupportedOperationException("Adding Node labels is not supported");
    }

    public void addNodeIdToLabel(long nodeId, NodeLabel nodeLabel) {
        throw new UnsupportedOperationException("Assigning Node labels to nodes is not supported");
    }
}
