/*
 * Copyright (c) 2017 "Neo4j, Inc." <http://neo4j.com>
 *
 * This file is part of Neo4j Graph Algorithms <http://github.com/neo4j-contrib/neo4j-graph-algorithms>.
 *
 * Neo4j Graph Algorithms is free software: you can redistribute it and/or modify
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

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.LongDoubleMap;
import com.carrotsearch.hppc.procedures.IntProcedure;
import org.neo4j.collection.primitive.PrimitiveIntIterable;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.RawValues;
import org.neo4j.graphdb.Direction;

import java.util.Collection;
import java.util.function.IntPredicate;

/**
 * virtual graph used by Louvain. This graph representation
 * does not aggregate degrees like heavy and huge do when using
 * undirected direction. The degree is just the sum of
 * incoming and outgoing degrees.
 *
 * @author mknblch
 */
public class LouvainGraph implements Graph {

    private final int nodeCount;
    private final IntObjectMap<? extends IntContainer> graph;
    private final LongDoubleMap weights;

    LouvainGraph(int newNodeCount, IntObjectMap<? extends IntContainer> graph, LongDoubleMap weights) {
        this.nodeCount = newNodeCount;
        this.graph = graph;
        this.weights = weights;
    }

    @Override
    public int toMappedNodeId(long nodeId) {
        // not implemented
        return -1;
    }

    @Override
    public long toOriginalNodeId(int nodeId) {
        // not implemented
        return -1L;
    }

    @Override
    public boolean contains(long nodeId) {
        // not implemented
        return false;
    }

    @Override
    public long nodeCount() {
        return nodeCount;
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, RelationshipConsumer consumer) {
        final IntContainer intCursors = graph.get(nodeId);
        if (null == intCursors) {
            return;
        }
        intCursors.forEach((IntProcedure) t -> consumer.accept(nodeId, t, -1));
    }

    @Override
    public double weightOf(int sourceNodeId, int targetNodeId) {
        return weights.getOrDefault(RawValues.combineIntInt(sourceNodeId, targetNodeId), 0);
    }

    @Override
    public String getType() {
        // not implemented
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void canRelease(boolean canRelease) {
    }

    @Override
    public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
        return ParallelUtil.batchIterables(batchSize, nodeCount);
    }

    @Override
    public int degree(int nodeId, Direction direction) {
        final IntContainer intContainer = graph.get(nodeId);
        if (null == intContainer) {
            return 0;
        }
        return intContainer.size();
    }

    @Override
    public void forEachNode(IntPredicate consumer) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public PrimitiveIntIterator nodeIterator() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public boolean exists(int sourceNodeId, int targetNodeId, Direction direction) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public RelationshipIntersect intersection() {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public int getTarget(int nodeId, int index, Direction direction) {
        return -1;
    }
}
