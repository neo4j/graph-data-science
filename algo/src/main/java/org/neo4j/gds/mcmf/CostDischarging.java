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
package org.neo4j.gds.mcmf;

import com.carrotsearch.hppc.BitSet;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;

import static org.neo4j.gds.mcmf.MinCostMaxFlow.TOLERANCE;

class CostDischarging {
    private final CostFlowGraph costFlowGraph;
    private final HugeDoubleArray excess;
    private final HugeDoubleArray prize;
    private final HugeLongArrayQueue workingQueue;
    private final BitSet inWorkingQueue;
    private final Arc[] filteredNeighbors;
    private final double freq;
    private double epsilon;

    private final GlobalRelabelling globalRelabelling;
    private double workSinceLastGR;

    private final long ALPHA = 6;
    private final long BETA = 12;


    CostDischarging(
        CostFlowGraph costFlowGraph,
        HugeDoubleArray excess,
        HugeDoubleArray prize,
        HugeLongArrayQueue workingQueue,
        BitSet inWorkingQueue,
        double epsilon,
        GlobalRelabelling globalRelabelling,
        double freq
    ) {
        this.costFlowGraph = costFlowGraph;
        this.excess = excess;
        this.prize = prize;
        this.workingQueue = workingQueue;
        this.inWorkingQueue = inWorkingQueue;
        this.epsilon = epsilon;
        this.filteredNeighbors = new Arc[(int)costFlowGraph.nodeCount()];
        this.globalRelabelling = globalRelabelling;
        this.workSinceLastGR = 0D;
        this.freq = freq;
    }

    void updateEpsilon(double epsilon) {
        this.epsilon = epsilon;
    }

    void dischargeUntilDone() {
        var relabelNumber = freq == 0 ? 0 : (ALPHA * costFlowGraph.nodeCount() + costFlowGraph.edgeCount() / freq);
        globalRelabelling.relabellingWithPriorityQueue(epsilon);
        while (!workingQueue.isEmpty()) {
            if(workSinceLastGR > relabelNumber) {
                globalRelabelling.relabellingWithPriorityQueue(epsilon);
                workSinceLastGR = 0;
            }
            var v = workingQueue.remove();
            inWorkingQueue.clear(v);
            dischargeSorted(v);
            workSinceLastGR += costFlowGraph.outDegree(v) + BETA;
        }
    }

    void dischargeSorted(long v) {
//        var k = sortNeighborhood(v);
        var k = sortNeighborhoodWithArray(v);

        for (var i = 0; i < k; i++) {
            var arc = filteredNeighbors[i];
            if(arc.almostReducedCost() + prize.get(v) >= 0) { //reduced cost is positive
                prize.set(v, -arc.almostReducedCost() - epsilon);
            }
            if (pushAndCheckIfEmptied(v, arc.t, arc.relIdx, arc.residualCapacity, arc.isReverse)) {
                break;
            }
        }
    }

    int sortNeighborhoodWithArray(long v) {
        var p = new MutableDouble(0);
        var k = new MutableInt(0);
        return costFlowGraph.forEachRelationship(v, (s, t, relIdx, residualCapacity, cost, isReverse) -> {
            if (residualCapacity > 0) {
                var almostReducedCost = cost - prize.get(t);
                if(almostReducedCost + prize.get(v) < 0) {
                    if (pushAndCheckIfEmptied(v, t, relIdx, residualCapacity, isReverse)) {
                        return false;
                    }
                    return true;
                }
                int i = k.intValue();
                Arc prev;
                if(p.doubleValue() < excess.get(v) || almostReducedCost < filteredNeighbors[i-1].almostReducedCost()) {
                    p.add(residualCapacity);
                    for(; i > 0; i--) {
                        prev = filteredNeighbors[i-1];
                        if (prev.almostReducedCost > almostReducedCost) {
//                            filteredNeighbors[i] = prev; //shift down
                            if (p.doubleValue() - prev.residualCapacity >= excess.get(v)) {
                                p.subtract(prev.residualCapacity);
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                    k.setValue(i+1); //cut-off the last elements that get pushed out
                    for(; i > 0; i--){
                        //shift the rest until free spot for prev
                        prev = filteredNeighbors[i-1];
                        if(prev.almostReducedCost > almostReducedCost){
                            filteredNeighbors[i] = prev; //shift down
                        } else {
                            break;
                        }
                    }
                    filteredNeighbors[i] = new Arc(t, relIdx, residualCapacity, almostReducedCost, isReverse);
                }

            }
            return true;
        }) ? k.intValue() : 0;
    }

     boolean pushAndCheckIfEmptied(long s, long t, long r, double residualCapacity, boolean isReverse) {
        var delta = Math.min(excess.get(s), residualCapacity);
        costFlowGraph.push(r, delta, isReverse);
        excess.addTo(s, -delta);
        excess.addTo(t, delta);

        if(excess.get(t) > TOLERANCE) {
            if(!inWorkingQueue.getAndSet(t)) {
                workingQueue.add(t);
            }
        }
        return excess.get(s) < TOLERANCE;
    }

     record Arc(long t, long relIdx, double residualCapacity, double almostReducedCost, boolean isReverse) implements Comparable<Arc> {

        @Override
        public int compareTo(@NotNull CostDischarging.Arc o) {
            int c = Double.compare(this.almostReducedCost, o.almostReducedCost);
            if (c != 0) return c;
            return Long.compare(this.relIdx, o.relIdx);
        }
    }
}
