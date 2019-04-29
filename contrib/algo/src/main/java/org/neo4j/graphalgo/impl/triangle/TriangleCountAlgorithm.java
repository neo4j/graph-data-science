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

import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.HugeGraph;
import org.neo4j.graphalgo.core.heavyweight.HeavyGraph;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.TerminationFlag;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

/**
 *
 * Triangle counting and coefficient
 *
 * https://epubs.siam.org/doi/pdf/10.1137/1.9781611973198.1
 * http://www.cse.cuhk.edu.hk/~jcheng/papers/triangle_kdd11.pdf
 * https://i11www.iti.kit.edu/extra/publications/sw-fclt-05_t.pdf
 * http://www.math.cmu.edu/~ctsourak/tsourICDM08.pdf
 *
 * @author mknblch
 */
public interface TriangleCountAlgorithm {

    /**
     * get number of triangles in the graph
     * @return
     */
    long getTriangleCount();

    /**
     * get average clustering coefficient
     * @return
     */
    double getAverageCoefficient();

    /**
     * get nodeid to triangle-count mapping
     * @param <V>
     * @return
     */
    <V> V getTriangles();

    /**
     * get nodeId to clustering coefficient mapping
     * @param <V>
     * @return
     */
    <V> V getCoefficients();

    /**
     * return stream of triples of original node id, number of triangles and clustering coefficient mapping
     * @return
     */
    Stream<Result> resultStream();

    TriangleCountAlgorithm withProgressLogger(ProgressLogger wrap);

    TriangleCountAlgorithm withTerminationFlag(TerminationFlag wrap);

    /**
     * release inner data structures
     * @return
     */
    TriangleCountAlgorithm release();

    /**
     * compute triangle count
     * @return
     */
    TriangleCountAlgorithm compute();

    static double calculateCoefficient(int triangles, int degree) {
        if (triangles == 0) {
            return 0.0;
        }
        return ((double) (triangles << 1)) / (degree * (degree - 1));
    }

    /**
     * result type
     */
    class Result {

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

    /**
     * create an instance of the triangle count algo based on which kind of graph
     * is given
     * @return triangle count algo
     */
    static TriangleCountAlgorithm instance(Graph graph, ExecutorService pool, int concurrency) {
        if (graph instanceof HugeGraph || graph instanceof HeavyGraph) {
            return new IntersectingTriangleCount(graph, pool, concurrency, AllocationTracker.create());
        } else {
            return new TriangleCountQueue(graph, pool, concurrency);
        }
    }

}
