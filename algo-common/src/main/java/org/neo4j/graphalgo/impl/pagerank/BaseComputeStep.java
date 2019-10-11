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
package org.neo4j.graphalgo.impl.pagerank;

import org.neo4j.graphalgo.api.Degrees;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.RelationshipIterator;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.core.utils.mem.MemoryUsage;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeCursor;
import org.neo4j.graphdb.Direction;

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

    long[] starts;
    private int[] lengths;
    protected double tolerance;
    private long[] sourceNodeIds;
    final RelationshipIterator relationshipIterator;
    final Degrees degrees;
    private final AllocationTracker tracker;

    private final double alpha;
    final double dampingFactor;

    final Direction direction;

    private final HugeCursor<double[]> cursor;
    double[] pageRank;
    double[] deltas;
    float[][] nextScores;
    float[][] prevScores;

    final long startNode;
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
            HugeCursor<double[]> cursor
    ) {
        this(
                dampingFactor,
                PageRank.DEFAULT_TOLERANCE,
                sourceNodeIds,
                graph,
                tracker,
                partitionSize,
                startNode,
                cursor
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
            HugeCursor<double[]> cursor
    ) {
        this.dampingFactor = dampingFactor;
        this.alpha = 1.0 - dampingFactor;
        this.tolerance = tolerance;
        this.sourceNodeIds = sourceNodeIds;
        this.relationshipIterator = graph.concurrentCopy();
        this.degrees = graph;
        this.direction = graph.getLoadDirection();
        this.tracker = tracker;
        this.partitionSize = partitionSize;
        this.startNode = startNode;
        this.endNode = startNode + (long) partitionSize;
        this.cursor = cursor;
        state = S_INIT;
    }

    static MemoryEstimation estimateMemory(
            final int partitionSize,
            final Class<?> computeStep) {
        return MemoryEstimations.builder(computeStep)
                .perThread("nextScores[] wrapper", MemoryUsage::sizeOfObjectArray)
                .perThread("inner nextScores[][]", sizeOfFloatArray(partitionSize))
                .fixed("pageRank[]", sizeOfDoubleArray(partitionSize))
                .fixed("deltas[]", sizeOfDoubleArray(partitionSize))
                .build();
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
        double initialValue = initialValue();
        if (sourceNodeIds.length == 0) {
            Arrays.fill(partitionRank, initialValue);
        } else {
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

        int length = prevScores[0].length;
        for (int i = 0; i < length; i++) {
            double sum = 0.0;
            for (int j = 0; j < scoreDim; j++) {
                float[] scores = prevScores[j];
                sum += (double) scores[i];
                scores[i] = 0F;
            }
            double delta = dampingFactor * sum;
            if (delta > tolerance) {
                shouldBreak = false;
            }
            pageRank[i] += delta;
            deltas[i] = delta;
        }

        return shouldBreak;
    }

    public float[][] nextScores() {
        return nextScores;
    }

    public double[] pageRank() {
        while (cursor.next()) {
            int prIndex = 0;
            double[] array = cursor.array;
            int offset = cursor.offset;
            int limit = cursor.limit;
            for (int j = offset; j < limit; prIndex++, j++) {
                array[j] = pageRank[prIndex];
            }
            // TODO: should the cursor release after flushing?
        }
        return pageRank;
    }

    public double[] deltas() { return deltas;}

    @Override
    public boolean partitionIsStable() {
        return shouldBreak;
    }
}
