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

package org.neo4j.graphalgo.impl.spanningTrees;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.RelationshipWithPropertyConsumer;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;

public class SpanningGraph implements Graph {

    private final Graph graph;
    private final SpanningTree spanningTree;

    public SpanningGraph(Graph graph, SpanningTree spanningTree) {
        this.graph = graph;
        this.spanningTree = spanningTree;
    }

    @Override
    public long relationshipCount() {
        AtomicInteger relationshipCount = new AtomicInteger();
        spanningTree.forEach((sourceNodeId, targetNodeId) -> {
            relationshipCount.incrementAndGet();
            return true;
        });
        return relationshipCount.get();
    }

    @Override
    public boolean isUndirected() {
        return true;
    }

    @Override
    public boolean hasRelationshipProperty() {
        return graph.hasRelationshipProperty();
    }

    @Override
    public Direction getLoadDirection() {
        return graph.getLoadDirection();
    }

    @Override
    public void canRelease(boolean canRelease) {
        graph.canRelease(canRelease);
    }

    @Override
    public RelationshipIntersect intersection() {
        return graph.intersection();
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        return graph.batchIterables(batchSize);
    }

    @Override
    public int degree(long nodeId, Direction direction) {
        return 1;
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        return graph.toMappedNodeId(nodeId);
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        return graph.toOriginalNodeId(nodeId);
    }

    @Override
    public boolean contains(long nodeId) {
        return graph.contains(nodeId);
    }

    @Override
    public long nodeCount() {
        return graph.nodeCount();
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        graph.forEachNode(consumer);
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        return graph.nodeIterator();
    }

    @Override
    public NodeProperties nodeProperties(String type) {
        return graph.nodeProperties(type);
    }

    @Override
    public Set<String> availableNodeProperties() {
        return graph.availableNodeProperties();
    }

    @Override
    public long getTarget(long nodeId, long index, Direction direction) {
        return spanningTree.parent[(int) nodeId];
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        forEachRelationship(
            nodeId,
            direction,
            0.0,
            (sourceNodeId, targetNodeId, property) -> consumer.accept(sourceNodeId, targetNodeId)
        );
    }

    @Override
    public void forEachRelationship(
        long nodeId,
        Direction direction,
        double fallbackValue,
        RelationshipWithPropertyConsumer consumer
    ) {
        int parent = spanningTree.parent[Math.toIntExact(nodeId)];
        if (parent != -1) {
            consumer.accept(parent, nodeId, fallbackValue);
        }
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        int source = Math.toIntExact(sourceNodeId);
        int target = Math.toIntExact(targetNodeId);
        return spanningTree.parent[source] != -1 || spanningTree.parent[target] != -1;
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId, double fallbackValue) {
        return fallbackValue;
    }

    @Override
    public double relationshipProperty(long sourceNodeId, long targetNodeId) {
        return 0;
    }
}
