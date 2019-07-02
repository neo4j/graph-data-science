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
package org.neo4j.graphalgo.core.heavyweight;

import org.apache.lucene.util.ArrayUtil;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.graphalgo.api.IntersectionConsumer;
import org.neo4j.graphalgo.api.RelationshipConsumer;
import org.neo4j.graphalgo.api.WeightedRelationshipConsumer;
import org.neo4j.graphalgo.core.utils.BitUtil;
import org.neo4j.graphalgo.core.utils.Intersections;
import org.neo4j.graphalgo.core.utils.ParallelUtil;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphdb.Direction;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.function.LongPredicate;

import static org.neo4j.graphalgo.core.heavyweight.HeavyGraph.checkSize;
import static org.neo4j.graphalgo.core.utils.ArrayUtil.LINEAR_SEARCH_LIMIT;
import static org.neo4j.graphalgo.core.utils.ArrayUtil.binarySearch;
import static org.neo4j.graphalgo.core.utils.ArrayUtil.binarySearchIndex;
import static org.neo4j.graphalgo.core.utils.ArrayUtil.linearSearch;
import static org.neo4j.graphalgo.core.utils.ArrayUtil.linearSearchIndex;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfFloatArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfIntArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfObjectArray;

/**
 * Relation Container built of multiple arrays. The node capacity must be constant and the node IDs have to be
 * smaller then the capacity. The number of relations per node is limited only to the maximum array size of the VM
 * and connections can be added dynamically.
 *
 * @author mknblch
 */
@SuppressWarnings({"DesignForExtension", "LocalCanBeFinal"})
public class AdjacencyMatrix {

    private static final int[] EMPTY_INTS = new int[0];
    private static final float[] EMPTY_FLOATS = new float[0];

    /**
     * mapping from nodeId to outgoing degree
     */
    private final int[] outOffsets;
    /**
     * mapping from nodeId to incoming degree
     */
    private final int[] inOffsets;
    /**
     * matrix nodeId x [outgoing edge-relationIds..]
     */
    private final int[][] outgoing;
    /**
     * matrix nodeId x [incoming edge-relationIds..]
     */
    private final int[][] incoming;
    /**
     * list of weights per nodeId, encoded as {@link Float#floatToIntBits(float).
     * Representation is sparse, missing values are written as 0
     */
    private final float[][] outgoingWeights;
    /**
     * list of weights per nodeId, encoded as {@link Float#floatToIntBits(float).
     * Representation is sparse, missing values are written as 0
     */
    private final float[][] incomingWeights;
    private final double defaultWeight;

    private boolean sorted = false;

    private final AllocationTracker tracker;

    public AdjacencyMatrix(
            int nodeCount,
            boolean withWeights,
            double defaultWeight,
            boolean sorted,
            AllocationTracker tracker) {
        this(nodeCount, true, true, withWeights, defaultWeight, sorted, true, tracker);
    }

    AdjacencyMatrix(
            int nodeCount,
            boolean withIncoming,
            boolean withOutgoing,
            boolean withWeights,
            double defaultWeight,
            boolean sorted,
            boolean preFill,
            AllocationTracker tracker) {
        long allocated = 0L;
        if (withOutgoing) {
            allocated += sizeOfIntArray(nodeCount);
            allocated += sizeOfObjectArray(nodeCount);
            this.outOffsets = new int[nodeCount];
            this.outgoing = new int[nodeCount][];
            if (preFill) {
                Arrays.fill(outgoing, EMPTY_INTS);
            }
            if (withWeights) {
                allocated += sizeOfObjectArray(nodeCount);
                this.outgoingWeights = new float[nodeCount][];
                if (preFill) {
                    Arrays.fill(outgoingWeights, EMPTY_FLOATS);
                }
            } else {
                this.outgoingWeights = null;
            }
        } else {
            this.outOffsets = null;
            this.outgoing = null;
            this.outgoingWeights = null;
        }
        if (withIncoming) {
            allocated += sizeOfIntArray(nodeCount);
            allocated += sizeOfObjectArray(nodeCount);
            this.inOffsets = new int[nodeCount];
            this.incoming = new int[nodeCount][];
            if (preFill) {
                Arrays.fill(incoming, EMPTY_INTS);
            }
            if (withWeights) {
                allocated += sizeOfObjectArray(nodeCount);
                this.incomingWeights = new float[nodeCount][];
                if (preFill) {
                    Arrays.fill(incomingWeights, EMPTY_FLOATS);
                }
            } else {
                this.incomingWeights = null;
            }
        } else {
            this.inOffsets = null;
            this.incoming = null;
            this.incomingWeights = null;
        }
        tracker.add(allocated);
        this.defaultWeight = defaultWeight;
        this.sorted = sorted;
        this.tracker = tracker;
    }

    public Direction getLoadDirection() {
        if (inOffsets != null && outOffsets != null) {
            return Direction.BOTH;
        } else if (inOffsets != null) {
            return Direction.INCOMING;
        } else {
            assert(outOffsets != null);
            return Direction.OUTGOING;
        }
    }

    public static MemoryEstimation memoryEstimation(
            boolean incoming,
            boolean outgoing,
            boolean undirected,
            boolean withWeights) {

        return MemoryEstimations
                .builder(AdjacencyMatrix.class)
                .perGraphDimension("buffers", dim -> {
                    int nodeCount = dim.nodeCountAsInt();
                    long rels = undirected ? dim.maxRelCount() << 1 : dim.maxRelCount();
                    int avgDegree = (int) Math.min(BitUtil.ceilDiv(rels, nodeCount), Integer.MAX_VALUE);
                    long perDirection = 0L;
                    perDirection += sizeOfIntArray(nodeCount);
                    perDirection += sizeOfObjectArray(nodeCount);
                    perDirection += nodeCount * sizeOfIntArray(avgDegree);
                    if (withWeights) {
                        perDirection += sizeOfObjectArray(nodeCount);
                        perDirection += nodeCount * sizeOfFloatArray(avgDegree);
                    }

                    long usage = 0L;
                    if (outgoing || undirected) {
                        usage += perDirection;
                    }
                    if (incoming && !undirected) {
                        usage += perDirection;
                    }

                    return usage;
                })
                .build();
    }

    /**
     * initialize array for outgoing connections
     */
    public int[] armOut(int sourceNodeId, int degree) {
        return armAny(sourceNodeId, degree, outgoing, outgoingWeights);
    }

    /**
     * initialize array for incoming connections
     */
    public int[] armIn(int targetNodeId, int degree) {
        return armAny(targetNodeId, degree, incoming, incomingWeights);
    }

    private int[] armAny(int nodeId, int degree, int[][] targets, float[][] weights) {
        if (degree > 0) {
            tracker.add(sizeOfIntArray(degree));
            targets[nodeId] = new int[degree];
            if (weights != null) {
                tracker.add(sizeOfFloatArray(degree));
                weights[nodeId] = new float[degree];
            }
        }
        return targets[nodeId];
    }

    /**
     * get weight storage for incoming weights
     */
    float[] getInWeights(int targetNodeId) {
        if (incomingWeights != null) {
            return incomingWeights[targetNodeId];
        }
        return null;
    }

    /**
     * get weight storage for outgoing weights
     */
    float[] getOutWeights(int targetNodeId) {
        if (outgoingWeights != null) {
            return outgoingWeights[targetNodeId];
        }
        return null;
    }

    void setOutDegree(int nodeId, final int degree) {
        outOffsets[nodeId] = degree;
    }

    void setInDegree(int nodeId, final int degree) {
        inOffsets[nodeId] = degree;
    }

    /**
     * grow array for outgoing connections
     */
    private void growOut(int sourceNodeId, int length) {
        assert length >= 0 : "size must be positive (got " + length + "): likely integer overflow?";
        if (outgoing[sourceNodeId].length < length) {
            outgoing[sourceNodeId] = growArray(outgoing[sourceNodeId], length);
        }
    }

    /**
     * grow array for outgoing weights
     */
    private void growOutWeights(int sourceNodeId, int length) {
        assert length >= 0 : "size must be positive (got " + length + "): likely integer overflow?";
        if (outgoingWeights[sourceNodeId].length < length) {
            outgoingWeights[sourceNodeId] = growArray(outgoingWeights[sourceNodeId], length);
        }
    }

    /**
     * grow array for incoming connections
     */
    private void growIn(int targetNodeId, int length) {
        assert length >= 0 : "size must be positive (got " + length + "): likely integer overflow?";
        if (incoming[targetNodeId].length < length) {
            incoming[targetNodeId] = growArray(incoming[targetNodeId], length);
        }
    }

    /**
     * grow array for incoming weights
     */
    private void growInWeights(int targetNodeId, int length) {
        assert length >= 0 : "size must be positive (got " + length + "): likely integer overflow?";
        if (incomingWeights[targetNodeId].length < length) {
            incomingWeights[targetNodeId] = growArray(incomingWeights[targetNodeId], length);
        }
    }

    private int[] growArray(int[] array, int length) {
        int newSize = ArrayUtil.oversize(length, Integer.BYTES);
        tracker.add(sizeOfIntArray(newSize) - sizeOfIntArray(array.length));
        return Arrays.copyOf(array, newSize);
    }

    private float[] growArray(float[] array, int length) {
        int newSize = ArrayUtil.oversize(length, Float.BYTES);
        tracker.add(sizeOfFloatArray(newSize) - sizeOfFloatArray(array.length));
        return Arrays.copyOf(array, newSize);
    }

    /**
     * add outgoing relation
     */
    public void addOutgoing(long sourceNodeId, long targetNodeId) {
        checkSize(sourceNodeId, targetNodeId);
        addOutgoing((int) sourceNodeId, (int) targetNodeId);
    }

    private void addOutgoing(int sourceNodeId, int targetNodeId) {
        final int degree = outOffsets[sourceNodeId];
        final int nextDegree = degree + 1;
        growOut(sourceNodeId, nextDegree);
        outgoing[sourceNodeId][degree] = targetNodeId;
        outOffsets[sourceNodeId] = nextDegree;
    }

    /**
     * add weight to an outgoing relation
     */
    public void addOutgoingWithWeight(int sourceNodeId, int targetNodeId, double weight) {
        final int degree = outOffsets[sourceNodeId];
        final int nextDegree = degree + 1;
        growOut(sourceNodeId, nextDegree);
        outgoing[sourceNodeId][degree] = targetNodeId;
        addOutgoingWeight(sourceNodeId, degree, weight);
        outOffsets[sourceNodeId] = nextDegree;
    }

    /**
     * add weight to a specific outgoing relation
     */
    public void addOutgoingWeight(int sourceNodeId, int index, double weight) {
        growOutWeights(sourceNodeId, index + 1);
        outgoingWeights[sourceNodeId][index] = (float) weight;
    }

    /**
     * get the weight from an outgoing relation
     */
    public double getOutgoingWeight(int sourceNodeId, int index) {
        return (double) outgoingWeights[sourceNodeId][index];
    }

    /**
     * checks for outgoing target node
     */
    public boolean hasOutgoing(long sourceNodeId, long targetNodeId) {
        checkSize(sourceNodeId, targetNodeId);
        return hasOutgoing((int) sourceNodeId, (int) targetNodeId);
    }

    private boolean hasOutgoing(int sourceNodeId, int targetNodeId) {

        final int degree = outOffsets[sourceNodeId];
        final int[] rels = outgoing[sourceNodeId];

        if (sorted && degree > LINEAR_SEARCH_LIMIT) {
            return binarySearch(rels, degree, targetNodeId);
        }

        return linearSearch(rels, degree, targetNodeId);
    }

    int getTargetOutgoing(long nodeId, long index) {
        checkSize(nodeId, index);
        return getTargetOutgoing((int) nodeId, (int) index);
    }

    /**
     * checks for outgoing target node
     */
    public int outgoingIndex(int sourceNodeId, int targetNodeId) {

        final int degree = outOffsets[sourceNodeId];
        final int[] rels = outgoing[sourceNodeId];

        if (sorted && degree > LINEAR_SEARCH_LIMIT) {
            return binarySearchIndex(rels, degree, targetNodeId);
        }

        return linearSearchIndex(rels, degree, targetNodeId);
    }

    private int getTargetOutgoing(int nodeId, int index) {
        final int degree = outOffsets[nodeId];
        if (index < 0 || index >= degree) {
            return -1;
        }
        return outgoing[nodeId][index];
    }

    int getTargetIncoming(long nodeId, long index) {
        checkSize(nodeId, index);
        return getTargetIncoming((int) nodeId, (int) index);
    }

    private int getTargetIncoming(int nodeId, int index) {
        final int degree = inOffsets[nodeId];
        if (index < 0 || index >= degree) {
            return -1;
        }
        return incoming[nodeId][index];
    }

    int getTargetBoth(long nodeId, long index) {
        checkSize(nodeId, index);
        return getTargetBoth((int) nodeId, (int) index);
    }

    private int getTargetBoth(int nodeId, int index) {
        final int outDegree = outOffsets[nodeId];
        if (index >= 0 && index < outDegree) {
            return outgoing[nodeId][index];
        } else {
            index -= outDegree;
            final int inDegree = inOffsets[nodeId];
            if (index >= 0 && index < inDegree) {
                return incoming[nodeId][index];
            }
        }
        return -1;
    }


    /**
     * checks for incoming target node
     */
    boolean hasIncoming(long sourceNodeId, long targetNodeId) {
        checkSize(sourceNodeId, targetNodeId);
        return hasIncoming((int) sourceNodeId, (int) targetNodeId);
    }

    private boolean hasIncoming(int sourceNodeId, int targetNodeId) {

        final int degree = inOffsets[sourceNodeId];
        final int[] rels = incoming[sourceNodeId];

        if (sorted && degree > LINEAR_SEARCH_LIMIT) {
            return binarySearch(rels, degree, targetNodeId);
        }

        return linearSearch(rels, degree, targetNodeId);
    }

    /**
     * checks for incoming target node
     */
    public int incomingIndex(int sourceNodeId, int targetNodeId) {

        final int degree = inOffsets[sourceNodeId];
        final int[] rels = incoming[sourceNodeId];

        if (sorted && degree > LINEAR_SEARCH_LIMIT) {
            return binarySearchIndex(rels, degree, targetNodeId);
        }

        return linearSearchIndex(rels, degree, targetNodeId);
    }

    /**
     * add incoming relation
     */
    public void addIncoming(int sourceNodeId, int targetNodeId) {
        final int degree = inOffsets[targetNodeId];
        final int nextDegree = degree + 1;
        growIn(targetNodeId, nextDegree);
        incoming[targetNodeId][degree] = sourceNodeId;
        inOffsets[targetNodeId] = nextDegree;
    }

    /**
     * add weight to an incoming relation
     */
    public void addIncomingWithWeight(int sourceNodeId, int targetNodeId, double weight) {
        final int degree = inOffsets[targetNodeId];
        final int nextDegree = degree + 1;
        growIn(targetNodeId, nextDegree);
        incoming[targetNodeId][degree] = sourceNodeId;
        addIncomingWeight(targetNodeId, degree, weight);
        inOffsets[targetNodeId] = nextDegree;
    }

    /**
     * add weight to a specific incoming relation
     */
    public void addIncomingWeight(int targetNodeId, int index, double weight) {
        growInWeights(targetNodeId, index + 1);
        incomingWeights[targetNodeId][index] = (float) weight;
    }


    /**
     * get the degree for node / direction
     *
     * @throws NullPointerException if the direction hasn't been loaded.
     */
    public int degree(long nodeId, Direction direction) {
        checkSize(nodeId);
        return degree((int) nodeId, direction);
    }

    private int degree(int nodeId, Direction direction) {
        switch (direction) {
            case OUTGOING: {
                return outOffsets[nodeId];
            }
            case INCOMING: {
                return inOffsets[nodeId];
            }
            default: {
                return inOffsets[nodeId] + outOffsets[nodeId];
            }
        }
    }

    /**
     * iterate over each edge at the given node using an unweighted consumer
     */
    public void forEach(long nodeId, Direction direction, RelationshipConsumer consumer) {
        checkSize(nodeId);
        forEach((int) nodeId, direction, consumer);
    }

    private void forEach(int nodeId, Direction direction, RelationshipConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                break;
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                break;
            default:
                forEachIncoming(nodeId, consumer);
                forEachOutgoing(nodeId, consumer);
                break;
        }
    }

    /**
     * iterate over each edge at the given node using a weighted consumer
     */
    void forEach(long nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        checkSize(nodeId);
        forEach((int) nodeId, direction, consumer);
    }

    private void forEach(int nodeId, Direction direction, WeightedRelationshipConsumer consumer) {
        switch (direction) {
            case OUTGOING:
                forEachOutgoing(nodeId, consumer);
                break;
            case INCOMING:
                forEachIncoming(nodeId, consumer);
                break;
            default:
                forEachOutgoing(nodeId, consumer);
                forEachIncoming(nodeId, consumer);
                break;
        }
    }

    double weightOf(final int sourceNodeId, final int targetNodeId) {
        if (outgoingWeights != null) {
            float[] weights = outgoingWeights[sourceNodeId];
            if (weights != null) {
                int index = outgoingIndex(sourceNodeId, targetNodeId);
                if (index >= 0 && index < weights.length) {
                    return weights[index];
                }
            }
        }
        if (incomingWeights != null) {
            float[] weights = incomingWeights[targetNodeId];
            if (weights != null) {
                int index = incomingIndex(targetNodeId, sourceNodeId);
                if (index >= 0 && index < weights.length) {
                    return weights[index];
                }
            }
        }
        return defaultWeight;
    }

    boolean hasWeights() {
        return outgoingWeights != null || incomingWeights != null;
    }

    public int capacity() {
        return outOffsets != null
                ? outOffsets.length
                : inOffsets != null
                ? inOffsets.length
                : 0;
    }

    private void forEachOutgoing(int nodeId, RelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, outs[i]);
        }
    }

    private void forEachIncoming(int nodeId, RelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] ins = incoming[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, ins[i]);
        }
    }

    private void forEachOutgoing(int nodeId, WeightedRelationshipConsumer consumer) {
        float[] weights;
        if (outgoingWeights == null || ((weights = outgoingWeights[nodeId]) == null)) {
            forEachOutgoingDefaultWeight(nodeId, consumer);
            return;
        }
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, outs[i], weights[i]);
        }
    }

    private void forEachOutgoingDefaultWeight(int nodeId, WeightedRelationshipConsumer consumer) {
        final int degree = outOffsets[nodeId];
        final int[] outs = outgoing[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, outs[i], defaultWeight);
        }
    }

    private void forEachIncoming(int nodeId, WeightedRelationshipConsumer consumer) {
        float[] weights;
        if (incomingWeights == null || ((weights = incomingWeights[nodeId]) == null)) {
            forEachIncomingDefaultWeight(nodeId, consumer);
            return;
        }
        final int degree = inOffsets[nodeId];
        final int[] neighbours = incoming[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, neighbours[i], weights[i]);
        }
    }

    private void forEachIncomingDefaultWeight(int nodeId, WeightedRelationshipConsumer consumer) {
        final int degree = inOffsets[nodeId];
        final int[] neighbours = incoming[nodeId];
        for (int i = 0; i < degree; i++) {
            consumer.accept(nodeId, neighbours[i], defaultWeight);
        }
    }

    public DegreeCheckingNodeIterator nodesWithRelationships(Direction direction) {
        if (direction == Direction.OUTGOING) {
            return new DegreeCheckingNodeIterator(outOffsets);
        } else {
            return new DegreeCheckingNodeIterator(inOffsets);
        }
    }

    public void sortIncoming(int node) {
        sortAny(node, inOffsets, incoming, incomingWeights);
    }

    public void sortOutgoing(int node) {
        sortAny(node, outOffsets, outgoing, outgoingWeights);
    }

    private void sortAny(int node, int[] degrees, int[][] targets, float[][] weights) {
        if (weights == null || weights[node] == null) {
            Arrays.sort(targets[node], 0, degrees[node]);
        } else {
            int degree = degrees[node];
            long[] targetsAndWeights = new long[degree];
            IndirectIntSort.sortWithoutDeduplication(targets[node], weights[node], targetsAndWeights, degree);
        }
    }

    public void sortAll(ExecutorService pool, int concurrency) {
        int nodes = outgoing.length;
        if (ParallelUtil.canRunInParallel(pool)) {
            ParallelUtil.iterateParallel(pool, nodes, concurrency, node -> {
                sortIncoming(node);
                sortOutgoing(node);
            });
        } else {
            for (int node = 0; node < nodes; ++node) {
                sortIncoming(node);
                sortOutgoing(node);
            }
        }
        sorted = true;
    }

    public void intersectAll(int nodeA, IntersectionConsumer consumer) {
        int outDegreeA = outOffsets[nodeA];
        int[] neighboursA = outgoing[nodeA];
        for (int i = 0; i < outDegreeA; i++) {
            int nodeB = neighboursA[i];
            int outDegreeB = outOffsets[nodeB];
            int[] neighboursB = outgoing[nodeB];
            int[] jointNeighbours = Intersections.getIntersection(neighboursA, outDegreeA, neighboursB, outDegreeB);
            for (int nodeC : jointNeighbours) {
                if (nodeB < nodeC) consumer.accept(nodeA, nodeB, nodeC);
            }
        }
    }

    static class DegreeCheckingNodeIterator {

        private final int[] array;

        DegreeCheckingNodeIterator(int[] array) {
            this.array = array != null ? array : EMPTY_INTS;
        }

        public void forEachNode(LongPredicate consumer) {
            for (int node = 0; node < array.length; node++) {
                if (array[node] > 0 && !consumer.test(node)) {
                    break;
                }
            }
        }

        public PrimitiveIntIterator nodeIterator() {
            return new PrimitiveIntIterator() {
                int index = findNext();

                @Override
                public boolean hasNext() {
                    return index < array.length;
                }

                @Override
                public int next() {
                    try {
                        return index;
                    } finally {
                        index = findNext();
                    }
                }

                private int findNext() {
                    int length = array.length;
                    for (int n = index + 1; n < length; n++) {
                        if (array[n] > 0) {
                            return n;
                        }
                    }
                    return length;
                }
            };
        }
    }
}
