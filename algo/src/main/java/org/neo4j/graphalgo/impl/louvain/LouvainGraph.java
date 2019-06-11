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
package org.neo4j.graphalgo.impl.louvain;

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.*;
import org.neo4j.graphalgo.core.huge.loader.IdMap;
import org.neo4j.graphalgo.core.utils.LazyBatchCollection;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.Set;
import java.util.function.LongPredicate;

/**
 * virtual graph used by Louvain. This graph representation
 * does not aggregate degrees like heavy and huge do when using
 * undirected direction. The degree is just the sum of
 * incoming and outgoing degrees.
 *
 * @author mknblch
 */
public final class LouvainGraph implements Graph {

    private final long nodeCount;
    private final SubGraph graph;
    private final SubWeights weights;

    LouvainGraph(long newNodeCount, SubGraph graph, SubWeights weights) {
        this.nodeCount = newNodeCount;
        this.graph = graph;
        this.weights = weights;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public long relationshipCount() {
        return RELATIONSHIP_COUNT_NOT_SUPPORTED;
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, RelationshipConsumer consumer) {
        graph.forEach(nodeId, consumer);
    }

    @Override
    public double weightOf(long sourceNodeId, long targetNodeId) {
        return weights.getOrDefault(sourceNodeId, targetNodeId);
    }

    @Override
    public Collection<PrimitiveLongIterable> batchIterables(int batchSize) {
        return LazyBatchCollection.of(
                nodeCount(),
                batchSize,
                IdMap.IdIterable::new);
    }

    @Override
    public int degree(long nodeId, Direction direction) {
        return graph.degree(nodeId);
    }

    @Override
    public void release() {
        weights.release();
        graph.release();
    }

    @Override
    public boolean contains(long nodeId) {
        throw new UnsupportedOperationException("contains is not supported.");
    }

    @Override
    public RelationshipIntersect intersection() {
        throw new UnsupportedOperationException("intersection is not supported.");
    }

    @Override
    public long toMappedNodeId(long nodeId) {
        throw new UnsupportedOperationException("toHugeMappedNodeId is not supported.");
    }

    @Override
    public long toOriginalNodeId(long nodeId) {
        throw new UnsupportedOperationException("toOriginalNodeId is not supported.");
    }

    @Override
    public void forEachNode(LongPredicate consumer) {
        throw new UnsupportedOperationException("forEachNode is not supported.");
    }

    @Override
    public PrimitiveLongIterator nodeIterator() {
        throw new UnsupportedOperationException("hugeNodeIterator is not supported.");
    }

    @Override
    public HugeWeightMapping nodeProperties(String type) {
        throw new UnsupportedOperationException("hugeNodeProperties is not supported.");
    }

    @Override
    public long getTarget(long nodeId, long index, Direction direction) {
        throw new UnsupportedOperationException("getTarget is not supported.");
    }

    @Override
    public void forEachRelationship(long nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        graph.forEach(nodeId, (s, t) -> consumer.accept(s, t, weights.getOrDefault(s, t)));
    }

    @Override
    public boolean exists(long sourceNodeId, long targetNodeId, Direction direction) {
        throw new UnsupportedOperationException("exists is not supported.");
    }

    @Override
    public Set<String> availableNodeProperties() {
        throw new UnsupportedOperationException("availableNodeProperties is not supported.");
    }

    @Override
    public String getType() {
        throw new UnsupportedOperationException("getType is not supported.");
    }

    @Override
    public void canRelease(boolean canRelease) {
        throw new UnsupportedOperationException("canRelease is not supported.");
    }
}
