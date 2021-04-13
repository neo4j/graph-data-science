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
package org.neo4j.graphalgo.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.ProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeDoubleArray;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfFloatArray;

public abstract class BaseComputeStep implements ComputeStep {
    private static final int S_INIT = 0;
    private static final int S_CALC = 1;
    private static final int S_SYNC = 2;
    private static final int S_NORM = 3;


    private int state;

    // start node ids for all partitions
    long[] starts;
    // size of all partitions
    private int[] lengths;

    protected double tolerance;
    private final long[] sourceNodeIds;
    final RelationshipIterator relationshipIterator;
    final Degrees degrees;
    private final AllocationTracker tracker;

    private final double alpha;
    final double dampingFactor;

    // stores rank for own partition = O(partition_size)
    double[] pageRank;
    // stores delta for own partition = O(partition_size)
    double[] deltas;
    // O(partition * partition_size) = O(node_count)
    float[][] nextScores;
    // O(partition * partition_size) = O(node_count)
    float[][] prevScores;

    final long startNode;
    final ProgressLogger progressLogger;
    final Graph graph;
    final long endNode;
    private final int partitionSize;
    double l2Norm;

    private boolean shouldBreak;

    BaseComputeStep(
        double dampingFactor,
        long[] sourceNodeIds,
        Graph graph,
        AllocationTracker tracker,
        int partitionSize,
        long startNode,
        ProgressLogger progressLogger
    ) {
        this(
            dampingFactor,
            PageRank.DEFAULT_TOLERANCE,
            sourceNodeIds,
            graph,
            tracker,
            partitionSize,
            startNode,
            progressLogger
        );
    }

    BaseComputeStep(
        double dampingFactor,
        double tolerance,
        long[] sourceNodeIds,
        Graph graph,
        AllocationTracker tracker,
        int partitionSize,
        long startNode,
        ProgressLogger progressLogger
    ) {
        this.dampingFactor = dampingFactor;
        // TODO why do we need this and not 1/n
        this.alpha = 1.0 - dampingFactor;
        this.tolerance = tolerance;
        this.sourceNodeIds = sourceNodeIds;
        this.graph = graph;
        this.relationshipIterator = graph.concurrentCopy();
        this.degrees = graph;
        this.tracker = tracker;
        this.partitionSize = partitionSize;
        this.startNode = startNode;
        this.progressLogger = progressLogger;
        this.endNode = startNode + (long) partitionSize;
        state = S_INIT;
    }

    public void setStarts(long[] starts, int[] lengths) {
        this.starts = starts;
        this.lengths = lengths;
    }

    @Override
    public void run() {
        if (state == S_CALC) {
            singleIteration();
            state = S_SYNC;
        } else if (state == S_SYNC) {
            this.shouldBreak = combineScores();
            state = S_NORM;
        } else if (state == S_NORM) {
            normalizeDeltas();
            state = S_CALC;
        } else if (state == S_INIT) {
            initialize();
            state = S_CALC;
        }
    }

    void normalizeDeltas() {}

    private void initialize() {
        this.nextScores = new float[starts.length][];
        Arrays.setAll(nextScores, i -> {
            int size = lengths[i];
            tracker.add(sizeOfFloatArray(size));
            return new float[size];
        });

        tracker.add(sizeOfDoubleArray(partitionSize) << 1);

        double[] partitionRank = new double[partitionSize];
        // alpha
        double initialValue = initialValue();
        if (sourceNodeIds.length == 0) {
            Arrays.fill(partitionRank, initialValue);
        } else {
            // personalized page rank initializes only source nodes with alpha
            Arrays.fill(partitionRank, 0.0);

            long[] partitionSourceNodeIds = LongStream.of(sourceNodeIds)
                .filter(sourceNodeId -> sourceNodeId >= startNode && sourceNodeId < endNode)
                .toArray();

            for (long sourceNodeId : partitionSourceNodeIds) {
                partitionRank[Math.toIntExact(sourceNodeId - this.startNode)] = initialValue;
            }
        }

        this.pageRank = partitionRank;
        this.deltas = Arrays.copyOf(partitionRank, partitionSize);
    }

    double initialValue() {
        return alpha;
    }

    abstract void singleIteration();

    @Override
    public void prepareNormalizeDeltas(double l2Norm) {
        this.l2Norm = l2Norm;
    }

    public void prepareNextIteration(float[][] prevScores) {
        this.prevScores = prevScores;
    }

    boolean combineScores() {
        assert prevScores != null;
        assert prevScores.length >= 1;

        int scoreDim = prevScores.length;
        float[][] prevScores = this.prevScores;

        boolean shouldBreak = true;

        // prev scores contains all partial scores sent by any other compute step
        // therefore each array in prev scores has the same length
        // Each column represents all scores for the node id
        int length = prevScores[0].length;
        for (int i = 0; i < length; i++) {
            // sum of partial scores
            double sum = 0.0;
            // sum up scores for each column
            for (int j = 0; j < scoreDim; j++) {
                float[] scores = prevScores[j];
                sum += scores[i];
                scores[i] = 0F;
            }
            // in Pregel we do delta = (jumpProbability / context.nodeCount()) + dampingFactor * sum;
            double delta = dampingFactor * degreeFactor() * sum;
            if (delta > tolerance) {
                shouldBreak = false;
            }
            pageRank[i] += delta;
            deltas[i] = delta;
        }

        return shouldBreak;
    }

    double degreeFactor() {
        return 1;
    }

    public float[][] nextScores() {
        return nextScores;
    }

    @Override
    public void getPageRankResult(HugeDoubleArray result) {
        result.copyFromArrayIntoSlice(pageRank, startNode, endNode);
    }

    public double[] deltas() { return deltas;}

    @Override
    public boolean partitionIsStable() {
        return shouldBreak;
    }
}
