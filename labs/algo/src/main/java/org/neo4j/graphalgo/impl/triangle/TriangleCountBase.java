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
package org.neo4j.graphalgo.impl.triangle;

import org.neo4j.graphalgo.LegacyAlgorithm;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphdb.Direction;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * TriangleCount counts the number of triangles in the Graph as well
 * as the number of triangles that passes through a node
 *
 * https://epubs.siam.org/doi/pdf/10.1137/1.9781611973198.1
 * http://www.cse.cuhk.edu.hk/~jcheng/papers/triangle_kdd11.pdf
 * https://i11www.iti.kit.edu/extra/publications/sw-fclt-05_t.pdf
 * http://www.math.cmu.edu/~ctsourak/tsourICDM08.pdf
 *
 * @author mknblch
 */
public abstract class TriangleCountBase<Coeff, Self extends TriangleCountBase<Coeff, Self>> extends LegacyAlgorithm<Self> {

    public static final Direction D = Direction.OUTGOING;

    private final AtomicInteger visitedNodes;
    private AtomicIntegerArray triangles;

    Graph graph;
    final int nodeCount;

    TriangleCountBase(Graph graph) {
        this.graph = graph;
        nodeCount = Math.toIntExact(graph.nodeCount());
        triangles = new AtomicIntegerArray(nodeCount);
        visitedNodes = new AtomicInteger();
    }

    /**
     * get stream of original nodeId to number of triangles of which the node is part of
     * @return stream of node-triangle pairs
     */
    public final Stream<Result> resultStream() {
        return IntStream.range(0, nodeCount)
                .mapToObj(i -> new Result(
                        graph.toOriginalNodeId(i),
                        triangles.get(i),
                        coefficient(i)));
    }

    /**
     * get number of triangles in the graph
     * @return
     */
    public abstract long getTriangleCount();

    /**
     * get array of nodeId to number of triangles mapping
     * @return
     */
    public final AtomicIntegerArray getTriangles() {
        return triangles;
    }

    /**
     * get coefficient for node
     * @return
     */
    abstract double coefficient(int node);

    /**
     * return nodeId to clustering coefficient mapping
     */
    public abstract Coeff getClusteringCoefficients();

    /**
     * get average clustering coefficient
     * @return
     */
    abstract double getAverageClusteringCoefficient();

    @SuppressWarnings("unchecked")
    @Override
    public final Self me() {
        return (Self) this;
    }

    @Override
    public void release() {
        graph = null;
        triangles = null;
    }

    /**
     * compute triangles
     * @return
     */
    public final Boolean compute() {
        visitedNodes.set(0);
        runCompute();
        return true;
    }

    abstract void runCompute();

    /**
     * store a single triangle
     * @param u
     * @param v
     * @param w
     */
    final void exportTriangle(int u, int v, int w) {
        triangles.incrementAndGet(u);
        triangles.incrementAndGet(v);
        triangles.incrementAndGet(w);
    }

    /**
     * progress logging
     */
    final void nodeVisited() {
        getProgressLogger().logProgress(visitedNodes.incrementAndGet(), nodeCount);
    }

    /**
     * calculate coefficient for nodeId in given direction
     */
    final double calculateCoefficient(int nodeId, Direction direction) {
        return calculateCoefficient(triangles.get(nodeId), graph.degree(nodeId, direction));
    }

    /**
     * calculate coefficient based using number of triangles and its degree
     * @param triangles
     * @param degree
     * @return
     */
    private double calculateCoefficient(int triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        return ((double) (triangles << 1)) / (degree * (degree - 1));
    }

    /**
     * result triple containing original node id, number of triangles and its coefficient
     */
    public static class Result {

        public final long nodeId;
        public final long triangles;
        public final double coefficient;

        public Result(long nodeId, long triangles, double coefficient) {
            this.nodeId = nodeId;
            this.triangles = triangles;
            this.coefficient = coefficient;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "nodeId=" + nodeId +
                    ", triangles=" + triangles +
                    ", coefficient=" + coefficient +
                    '}';
        }
    }
}
