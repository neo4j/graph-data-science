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
package org.neo4j.graphalgo.core.huge;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.PropertyMapping;
import org.neo4j.graphalgo.api.PropertyRelationshipConsumer;
import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.collection.Iterables;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;
import java.util.stream.Collectors;

public final class UnionGraph implements Graph {

    private final Graph first;
    private final Collection<? extends Graph> graphs;

    public static Graph of(Collection<? extends Graph> graphs) {
        if (graphs.isEmpty()) {
            throw new IllegalArgumentException("no graphs");
        }
        if (graphs.size() == 1) {
            return Iterables.single(graphs);
        }
        return new UnionGraph(graphs);
    }

    private UnionGraph(Collection<? extends Graph> graphs) {
        first = Iterables.first(graphs);
        this.graphs = graphs;
    }

    @Override
    public long nodeCount() {
        return first.nodeCount();
    }

    @Override
    public long relationshipCount() {
        return graphs.stream().mapToLong(Graph::relationshipCount).sum();
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(final int batchSize) {
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
    public PropertyMapping nodeProperties(final String type) {
        return first.nodeProperties(type);
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
    public boolean contains(final long nodeId) {
        return first.contains(nodeId);
    }

    @Override
    public double relationshipValue(final long sourceNodeId, final long targetNodeId, double fallbackValue) {
        return first.relationshipValue(sourceNodeId, targetNodeId, fallbackValue);
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        for (Graph graph : graphs) {
            graph.forEachRelationship(nodeId, direction, consumer);
        }
    }

    @Override
    public void forEachRelationship(
            long nodeId,
            Direction direction,
            double fallbackValue,
            PropertyRelationshipConsumer consumer) {
        for (Graph graph : graphs) {
            graph.forEachRelationship(nodeId, direction, fallbackValue, consumer);
        }
    }

    @Override
    public int degree(final long node, final Direction direction) {
        return Math.toIntExact(graphs.stream().mapToLong(g -> g.degree(node, direction)).sum());
    }

    @Override
    public void forEachIncoming(long node, final RelationshipConsumer consumer) {
        for (Graph graph : graphs) {
            graph.forEachIncoming(node, consumer);
        }
    }

    @Override
    public void forEachOutgoing(long node, final RelationshipConsumer consumer) {
        for (Graph graph : graphs) {
            graph.forEachOutgoing(node, consumer);
        }
    }

    @Override
    public Graph concurrentCopy() {
        return of(graphs.stream().map(graph -> (Graph) graph.concurrentCopy()).collect(Collectors.toList()));
    }

    @Override
    public RelationshipIntersect intersection() {
        throw new UnsupportedOperationException("#intersection is not supported for multiple relationship types");
    }

    /**
     * O(n) !
     */
    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        return graphs.stream().anyMatch(g -> g.exists(sourceNodeId, targetNodeId, direction));
    }

    /*
     * O(n) !
     */
    @Override
    public long getTarget(long sourceNodeId, long index, Direction direction) {
        return graphs.stream()
                .mapToLong(g -> g.getTarget(sourceNodeId, index, direction))
                .filter(t -> t != HugeGraph.GetTargetConsumer.TARGET_NOT_FOUND)
                .findFirst()
                .orElse(HugeGraph.GetTargetConsumer.TARGET_NOT_FOUND);
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
            graph.releaseProperties();
        }
    }

    @Override
    public void releaseProperties() {
        for (Graph graph : graphs) {
            graph.releaseProperties();
        }
    }

    @Override
    public Direction getLoadDirection() {
        return first.getLoadDirection();
    }

    @Override
    public boolean hasRelationshipProperty() {
        return first.hasRelationshipProperty();
    }

    @Override
    public boolean isUndirected() {
        return first.isUndirected();
    }
}
