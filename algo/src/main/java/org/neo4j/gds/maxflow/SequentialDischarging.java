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
package org.neo4j.gds.maxflow;

import com.carrotsearch.hppc.BitSet;
import org.apache.commons.lang3.LongRange;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.mcmf.MinCostFunctions;
import org.neo4j.gds.termination.TerminationFlag;

import static org.neo4j.gds.maxflow.MaxFlowFunctions.TERMINATION_BOUND;

class SequentialDischarging {
    private static final long ALPHA = 6;
    private static final long BETA = 12;
    private static final double TOLERANCE = 1e-10;
    private final FlowGraph flowGraph;
    private final HugeDoubleArray excess;
    private final HugeLongArray label;
    private final HugeLongArrayQueue workingQueue;
    private final BitSet inWorkingQueue;
    private final GlobalRelabeling globalRelabeling;
    private final double freq;
    private final Arc[] filteredNeighbors;
    private final GapDetector gapDetector;
    private final double totalExcess;
    private final ProgressTracker progressTracker;
    private long workSinceLastGR;
    private double excessAtDestinations;
    private final TerminationFlag terminationFlag;

    SequentialDischarging(
        FlowGraph flowGraph,
        HugeDoubleArray excess,
        HugeLongArray label,
        HugeLongArrayQueue workingQueue,
        BitSet inWorkingQueue,
        GlobalRelabeling globalRelabeling,
        GapDetector gapDetector,
        double freq,
        double excessAtDestinations,
        double totalExcess,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        this.flowGraph = flowGraph;
        this.excess = excess;
        this.label = label;
        this.workingQueue = workingQueue;
        this.inWorkingQueue = inWorkingQueue;
        this.globalRelabeling = globalRelabeling;
        this.gapDetector = gapDetector;
        this.freq = freq;
        this.excessAtDestinations = excessAtDestinations;
        this.totalExcess = totalExcess;
        this.progressTracker = progressTracker;
        this.filteredNeighbors = new Arc[(int) LongRange.of(0L, flowGraph.nodeCount() - 1)
            .toLongStream()
            .map(flowGraph::degree)
            .max()
            .getAsLong()]; //max (in+out)degree instead (theoretically IntMax + IntMax)
        this.workSinceLastGR = 0L;
        this.terminationFlag = terminationFlag;
    }

    void dischargeUntilDone() {
        final long relabelingNumber = freq > 0
            ? (long) ((ALPHA * flowGraph.nodeCount() + flowGraph.edgeCount()) / freq)
            : Long.MAX_VALUE;
        globalRelabeling.globalRelabeling();
        gapDetector.resetCounts();
        int terminationCheckingSteps=0;
        while (!workingQueue.isEmpty()) {
            if (++terminationCheckingSteps == TERMINATION_BOUND){
                terminationCheckingSteps=0;
                terminationFlag.assertRunning();
            }
            if (workSinceLastGR > relabelingNumber) {
                globalRelabeling.globalRelabeling();
                gapDetector.resetCounts();
                workSinceLastGR = 0;
            }
            var v = workingQueue.remove();
            inWorkingQueue.clear(v);
            discharge(v);
        }
    }

    private int sortAndPush(long v, long vLabel) {
        var p = new MutableDouble(0);
        var k = new MutableInt(0);
        return flowGraph.forEachRelationship(
            v, (s, t, relIdx, residualCapacity, isReverse) -> {
                if (MinCostFunctions.isResidualEdge(residualCapacity)) {
                    var tLabel = label.get(t);
                    if (tLabel == (vLabel - 1)) {
                        return !pushAndCheckIfEmpty(s, t, relIdx, residualCapacity, isReverse);
                    } else if (tLabel < flowGraph.nodeCount()) {
                        int i = k.intValue();
                        Arc prev;
                        if (p.doubleValue() < excess.get(v) || tLabel < filteredNeighbors[i - 1].label) {
                            p.add(residualCapacity);
                            for (; i > 0; i--) {
                                prev = filteredNeighbors[i - 1];
                                if (prev.label > tLabel) {
                                    if (p.doubleValue() - prev.residualCapacity > excess.get(v)) {
                                        p.subtract(prev.residualCapacity);
                                    } else {
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                            k.setValue(i + 1); //cut-off the last elements that get pushed out
                            for (; i > 0; i--) {
                                //shift the rest until free spot for prev
                                prev = filteredNeighbors[i - 1];
                                if (prev.label > tLabel) {
                                    filteredNeighbors[i] = prev; //shift down
                                } else {
                                    break;
                                }
                            }
                            filteredNeighbors[i] = new Arc(t, relIdx, residualCapacity, tLabel, isReverse);
                        }
                    }
                }
                return true;
            }
        ) ? k.intValue() : 0;
    }

    void discharge(long v) {
        var oldLabel = label.get(v);
        if (oldLabel >= flowGraph.nodeCount()) { //only used by gap-heuristic (if gap-relabeled after added to workingQueue)
            return;
        }

        var k = sortAndPush(v, oldLabel);

        if (excess.get(v) < TOLERANCE) {
            workSinceLastGR += flowGraph.outDegree(v) + BETA;
            return;
        }


        for (var i = 0; i < k; i++) {
            var arc = filteredNeighbors[i];

            var newLabel = arc.label() + 1;
            if (newLabel > oldLabel) { //same as !=
                boolean empty = gapDetector.moveFrom(v, oldLabel, newLabel);
                if (empty) {
                    gapDetector.relabel(oldLabel);
                }
                label.set(v, newLabel);
                oldLabel = newLabel;
            }

            if (pushAndCheckIfEmpty(v, arc.t(), arc.relIdx(), arc.residualCapacity(), arc.isReverse())) {
                workSinceLastGR += flowGraph.outDegree(v) + BETA;
                return;
            }
        }
    }


    private boolean pushAndCheckIfEmpty(long s, long t, long relIdx, double residualCapacity, boolean isReverse) {
        var delta = Math.min(excess.get(s), residualCapacity);
        flowGraph.push(relIdx, delta, isReverse);
        if (delta > TOLERANCE) {
            excess.addTo(s, -delta);
            excess.addTo(t, delta);
            if (label.get(t) == 0) {
                progressTracker.logProgress((long) (Math.ceil((excessAtDestinations + delta) * progressTracker.currentVolume() / totalExcess) - Math.ceil(
                    excessAtDestinations * progressTracker.currentVolume() / totalExcess)));
                excessAtDestinations += delta;
            } else if (!inWorkingQueue.getAndSet(t)) {
                workingQueue.add(t);
            }

            return excess.get(s) < TOLERANCE; //empties
        }{
            return false;
        }
    }


    record Arc(long t, long relIdx, double residualCapacity, long label, boolean isReverse) {
    }
}
