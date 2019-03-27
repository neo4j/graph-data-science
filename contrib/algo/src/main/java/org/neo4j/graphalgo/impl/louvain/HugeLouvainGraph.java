/**
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

import org.neo4j.collection.primitive.PrimitiveLongIterable;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.api.HugeRelationshipConsumer;
import org.neo4j.graphalgo.api.HugeWeightMapping;
import org.neo4j.graphalgo.api.HugeWeightedRelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.RelationshipIntersect;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.graphalgo.core.utils.paged.HugeObjectArray;
import org.neo4j.graphalgo.core.utils.paged.PagedLongDoubleMap;
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
public class HugeLouvainGraph implements HugeGraph {

    private final long nodeCount;
    private final HugeObjectArray<HugeLongArray> graph;
    private final PagedLongDoubleMap weights;

    HugeLouvainGraph(long newNodeCount, HugeObjectArray<HugeLongArray> graph, PagedLongDoubleMap weights) {
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
    public void forEachRelationship(long nodeId, Direction direction, HugeRelationshipConsumer consumer) {
        final HugeLongArray intCursors = graph.get(nodeId);
        if (null == intCursors) {
            return;
        }
        try (HugeCursor<long[]> cursor = intCursors.cursor(intCursors.newCursor())) {
            while (cursor.next()) {
                long[] array = cursor.array;
                for (int i = cursor.offset; i < cursor.limit; i++) {
                    long target = array[i];
                    if (!consumer.accept(nodeId, target)) {
                        return;
                    }
                }
            }
        }
    }

    @Override
    public double weightOf(final long sourceNodeId, final long targetNodeId) {
        // TODO
        //  return weights.getOrDefault(RawValues.combineIntInt(sourceNodeId, targetNodeId), 0);
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.weightOf is not implemented.");
    }

    // TODO:

    @Override
    public RelationshipIntersect intersection() {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.intersection is not implemented.");
    }

    @Override
    public Collection<PrimitiveLongIterable> hugeBatchIterables(final int batchSize) {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.hugeBatchIterables is not implemented.");
    }

    @Override
    public int degree(final long nodeId, final Direction direction) {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.degree is not implemented.");
    }

    @Override
    public long toHugeMappedNodeId(final long nodeId) {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.toHugeMappedNodeId is not implemented.");
    }

    @Override
    public long toOriginalNodeId(final long nodeId) {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.toOriginalNodeId is not implemented.");
    }

    @Override
    public void forEachNode(final LongPredicate consumer) {

    }

    @Override
    public PrimitiveLongIterator hugeNodeIterator() {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.hugeNodeIterator is not implemented.");
    }

    @Override
    public HugeWeightMapping hugeNodeProperties(final String type) {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.hugeNodeProperties is not implemented.");
    }

    @Override
    public long getTarget(final long nodeId, final long index, final Direction direction) {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.getTarget is not implemented.");
    }

    @Override
    public void forEachRelationship(
            final long nodeId, final Direction direction, final HugeWeightedRelationshipConsumer consumer) {

    }

    @Override
    public boolean exists(final long sourceNodeId, final long targetNodeId, final Direction direction) {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.exists is not implemented.");
    }

    @Override
    public Set<String> availableNodeProperties() {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.availableNodeProperties is not implemented.");
    }

    @Override
    public int getTarget(final int nodeId, final int index, final Direction direction) {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.getTarget is not implemented.");
    }

    @Override
    public void forEachRelationship(final int nodeId, final Direction direction, final RelationshipConsumer consumer) {

    }

    @Override
    public boolean exists(final int sourceNodeId, final int targetNodeId, final Direction direction) {
        throw new UnsupportedOperationException(
                "org.neo4j.graphalgo.impl.louvain.HugeLouvainGraph.exists is not implemented.");
    }

    @Override
    public void forEachRelationship(
            final int nodeId, final Direction direction, final WeightedRelationshipConsumer consumer) {

    }


    @Override
    public String getType() {
        // not implemented
        throw new IllegalStateException("not implemented");
    }

    @Override
    public void canRelease(boolean canRelease) {
    }

//    @Override
//    public Collection<PrimitiveIntIterable> batchIterables(int batchSize) {
//        return ParallelUtil.batchIterables(batchSize, nodeCount);
//    }

//    @Override
//    public int degree(int nodeId, Direction direction) {
//        final IntContainer intContainer = graph.get(nodeId);
//        if (null == intContainer) {
//            return 0;
//        }
//        return intContainer.size();
//    }

//    @Override
//    public void forEachNode(IntPredicate consumer) {
//        throw new UnsupportedOperationException("not implemented");
//    }
//
//    @Override
//    public PrimitiveIntIterator nodeIterator() {
//        throw new UnsupportedOperationException("not implemented");
//    }
//
//    @Override
//    public boolean exists(int sourceNodeId, int targetNodeId, Direction direction) {
//        throw new UnsupportedOperationException("not implemented");
//    }
//
//    @Override
//    public void forEachRelationship(int nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
//        throw new UnsupportedOperationException("not implemented");
//    }
//
//    @Override
//    public RelationshipIntersect intersection() {
//        throw new UnsupportedOperationException("not implemented");
//    }
//
//    @Override
//    public int getTarget(int nodeId, int index, Direction direction) {
//        return -1;
//    }
}
