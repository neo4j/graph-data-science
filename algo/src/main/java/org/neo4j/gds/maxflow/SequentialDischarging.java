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
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.Arrays;
import java.util.Comparator;

class SequentialDischarging {
    private final FlowGraph flowGraph;
    private final HugeDoubleArray excess;
    private final HugeLongArray label;
    private final HugeLongArrayQueue workingQueue;
    private final BitSet inWorkingQueue;
    private final GlobalRelabeling globalRelabeling;
    private final double freq;

    private final Arc[] filteredNeighbors;
    private long workSinceLastGR;
    private final GapDetector gapDetector;
    private double excessAtDestinations;
    private final double totalExcess;
    private final ProgressTracker progressTracker;

    private static final long ALPHA = 6;
    private static final long BETA = 12;
    private static final double TOLERANCE = 1e-10;

    SequentialDischarging(
        FlowGraph flowGraph,
        HugeDoubleArray excess,
        HugeLongArray label,
        HugeLongArrayQueue workingQueue,
        BitSet inWorkingQueue,
        GlobalRelabeling globalRelabeling,
        double freq,
        boolean useGapRelabelling,
        double excessAtDestinations,
        double totalExcess,
        ProgressTracker progressTracker
    ) {
        this.flowGraph = flowGraph;
        this.excess = excess;
        this.label = label;
        this.workingQueue = workingQueue;
        this.inWorkingQueue = inWorkingQueue;
        this.globalRelabeling = globalRelabeling;
        this.freq = freq;
        this.excessAtDestinations = excessAtDestinations;
        this.totalExcess = totalExcess;
        this.progressTracker = progressTracker;
        this.filteredNeighbors = new Arc[(int) LongRange.of(0L, flowGraph.nodeCount()-1).toLongStream().map(flowGraph::degree).max().getAsLong()]; //max (in+out)degree instead (theoretically IntMax + IntMax)
        this.workSinceLastGR = 0L;
        this.gapDetector = GapFactory.create(useGapRelabelling,flowGraph.nodeCount(),this.label);
    }

    void dischargeUntilDone() {
        final long relabelingNumber = freq > 0 ? (long) ((ALPHA * flowGraph.nodeCount() + flowGraph.edgeCount()) / freq) : Long.MAX_VALUE;
        globalRelabeling.globalRelabeling();
        gapDetector.resetCounts();

        while (!workingQueue.isEmpty()) {
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

    void discharge(long v) {
        var oldLabel = label.get(v);
        if(oldLabel >= flowGraph.nodeCount()) { //last check only used by gap-heuristic
            return;
        }

        var idx = new MutableInt(0);
        flowGraph.forEachRelationship(
            v, (s, t, relIdx, residualCapacity, isReverse) -> {
                if (residualCapacity > 0 && label.get(t) < flowGraph.nodeCount()) {
                    filteredNeighbors[idx.getAndIncrement()] = new Arc(t, relIdx, residualCapacity, label.get(t), isReverse);
                }
                return true;
            }
        );
        Arrays.sort(filteredNeighbors, 0, idx.intValue(), Comparator.comparing(Arc::label));

        for (var i = 0; i < idx.intValue(); i++) {

            var arc = filteredNeighbors[i];
            var t = arc.t();

            var newLabel = arc.label() + 1;
            if (newLabel > oldLabel) { //same as !=
                boolean empty = gapDetector.moveFrom(v, oldLabel, newLabel);
                if (empty) {
                    gapDetector.relabel(oldLabel);
                }
                label.set(v, newLabel);
                oldLabel = newLabel;
            }

            var delta = Math.min(excess.get(v), arc.residualCapacity());
            flowGraph.push(arc.relIdx(), delta, arc.isReverse());
            excess.addTo(v, -delta);
            excess.addTo(t, delta);

            if(arc.label() == 0) {
                progressTracker.logProgress((long) ( Math.ceil((excessAtDestinations+delta) * progressTracker.currentVolume() / totalExcess) - Math.ceil(excessAtDestinations * progressTracker.currentVolume() / totalExcess) ));
                excessAtDestinations += delta;
            } else if(!inWorkingQueue.getAndSet(t)) {
                workingQueue.add(t);
            }

            if(excess.get(v) < TOLERANCE) {
                break;
            }
        }
        workSinceLastGR += flowGraph.outDegree(v) + BETA;
    }




    record Arc(long t, long relIdx, double residualCapacity, long label, boolean isReverse) {}
}
