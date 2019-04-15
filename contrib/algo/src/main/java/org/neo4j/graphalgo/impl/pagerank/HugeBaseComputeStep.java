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
package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.HugeDegrees;
import org.neo4j.graphalgo.api.HugeRelationshipIterator;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;

import java.util.Arrays;
import java.util.stream.LongStream;

import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfDoubleArray;
import static org.neo4j.graphalgo.core.utils.paged.MemoryUsage.sizeOfFloatArray;

public abstract class HugeBaseComputeStep implements HugeComputeStep {
    private static final int S_INIT = 0;
    private static final int S_CALC = 1;
    private static final int S_SYNC = 2;
    private static final int S_NORM = 3;

    private int state;

    long[] starts;
    private int[] lengths;
    private long[] sourceNodeIds;
    final HugeRelationshipIterator relationshipIterator;
    final HugeDegrees degrees;
    private final AllocationTracker tracker;

    private final double alpha;
    final double dampingFactor;

    double[] pageRank;
    double[] deltas;
    float[][] nextScores;
    float[][] prevScores;

    final long startNode;
    final long endNode;
    private final int partitionSize;
    double l2Norm;

    HugeBaseComputeStep(
            double dampingFactor,
            long[] sourceNodeIds,
            HugeRelationshipIterator relationshipIterator,
            HugeDegrees degrees,
            AllocationTracker tracker,
            int partitionSize,
            long startNode) {
        this.dampingFactor = dampingFactor;
        this.alpha = 1.0 - dampingFactor;
        this.sourceNodeIds = sourceNodeIds;
        this.relationshipIterator = relationshipIterator.concurrentCopy();
        this.degrees = degrees;
        this.tracker = tracker;
        this.partitionSize = partitionSize;
        this.startNode = startNode;
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
            combineScores();
            state = S_NORM;
        } else if(state == S_NORM) {
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
        double initialValue = initialValue();
        if(sourceNodeIds.length == 0) {
            Arrays.fill(partitionRank, initialValue);
        } else {
            Arrays.fill(partitionRank,0.0);

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

    void combineScores() {
        assert prevScores != null;
        assert prevScores.length >= 1;

        int scoreDim = prevScores.length;
        float[][] prevScores = this.prevScores;

        int length = prevScores[0].length;
        for (int i = 0; i < length; i++) {
            double sum = 0.0;
            for (int j = 0; j < scoreDim; j++) {
                float[] scores = prevScores[j];
                sum += (double) scores[i];
                scores[i] = 0f;
            }
            double delta = dampingFactor * sum;
            pageRank[i] += delta;
            deltas[i] = delta;
        }
    }

    public float[][] nextScores() {
        return nextScores;
    }

    public double[] pageRank() {
        return pageRank;
    }

    public long[] starts() {
        return starts;
    }

    public double[] deltas() { return deltas;}
}
