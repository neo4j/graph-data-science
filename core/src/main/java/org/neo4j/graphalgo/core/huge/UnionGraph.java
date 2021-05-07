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
package org.neo4j.graphalgo.core.huge;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.graphalgo.NodeLabel;
import org.neo4j.graphalgo.RelationshipType;
import org.neo4j.graphalgo.api.CSRGraph;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipCursor;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphalgo.api.Relationships;
import org.neo4j.graphalgo.api.schema.GraphSchema;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterable;
import org.neo4j.graphalgo.core.utils.collection.primitive.PrimitiveLongIterator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class UnionGraph implements CSRGraph {

    private final CSRGraph first;
    private final List<? extends CSRGraph> graphs;
    private final Map<RelationshipType, Relationships.Topology> relationshipTypeTopologies;

    public static CSRGraph of(List<? extends CSRGraph> graphs) {
        if (graphs.isEmpty()) {
            throw new IllegalArgumentException("no graphs");
        }
        if (graphs.size() == 1) {
            return graphs.iterator().next();
        }
        return new UnionGraph(graphs);
    }

    private UnionGraph(List<? extends CSRGraph> graphs) {
        first = graphs.iterator().next();
        this.graphs = graphs;
        this.relationshipTypeTopologies = new HashMap<>();
        graphs.forEach(graph -> relationshipTypeTopologies.putAll(graph.relationshipTopologies()));
    }

    @Override
    public long nodeCount() {
        return first.nodeCount();
    }

    @Override
    public long rootNodeCount() {
        return first.nodeCount();
    }

    @Override
    public long highestNeoId() {
        return first.highestNeoId();
    }

    @Override
    public GraphSchema schema() {
        return graphs
            .stream()
            .map(Graph::schema)
            .reduce(GraphSchema::union)
            .get();
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
    public PrimitiveLongIterator nodeIterator() {
        return first.nodeIterator();
    }

    @Override
    public NodeProperties nodeProperties(final String propertyKey) {
        return first.nodeProperties(propertyKey);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return first.availableNodeProperties();
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return first.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return first.toOriginalNodeId(nodeId);
    }

    @Override
    public long toRootNodeId(long nodeId) {
        return first.toRootNodeId(nodeId);
    }

    @Override
    public boolean contains(final long nodeId) {
        return first.contains(nodeId);
    }

    @Override
    public double relationshipProperty(final long sourceNodeId, final long targetNodeId, double fallbackValue) {
        return first.relationshipProperty(sourceNodeId, targetNodeId, fallbackValue);
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        return first.relationshipProperty(sourceNodeId, targetNodeId);
    }

    @Override
    public Map<RelationshipType, Relationships.Topology> relationshipTopologies() {
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

    /**
     * O(n) !
     */
    @Override
    public boolean exists(long sourceNodeId, long targetNodeId) {
        return graphs.stream().anyMatch(g -> g.exists(sourceNodeId, targetNodeId));
    }

    /*
     * O(n) !
     */
    @Override
    public long getTarget(long sourceNodeId, long index) {
        var currentIndex = 0;

        for (Graph graph : graphs) {
            var degree = graph.degree(sourceNodeId);
            if (currentIndex + degree > index) {
                return graph.getTarget(sourceNodeId, index - currentIndex);
            }
            currentIndex += degree;
        }

        return HugeGraph.GetTargetConsumer.TARGET_NOT_FOUND;
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
    public boolean isUndirected() {
        return graphs.stream().allMatch(Graph::isUndirected);
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
            .map(Relationships.Topology::adjacencyList)
            .collect(Collectors.toList());
        return new CompositeAdjacencyList(adjacencies);
    }

    @Override
    public Set<NodeLabel> nodeLabels(long nodeId) {
        return first.nodeLabels(nodeId);
    }

    @Override
    public void forEachNodeLabel(long nodeId, NodeLabelConsumer consumer) {
        first.forEachNodeLabel(nodeId, consumer);
    }

    @Override
    public Set<NodeLabel> availableNodeLabels() {
        return first.availableNodeLabels();
    }

    @Override
    public boolean hasLabel(long nodeId, NodeLabel label) {
        return first.hasLabel(nodeId, label);
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
}
