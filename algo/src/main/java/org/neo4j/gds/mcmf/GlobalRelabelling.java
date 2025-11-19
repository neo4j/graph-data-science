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

import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeAtomicBitSet;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

import java.util.concurrent.atomic.AtomicLong;

import static org.neo4j.gds.mcmf.MinCostMaxFlow.TOLERANCE;

public class GlobalRelabelling {
    final CostFlowGraph costFlowGraph;
    final HugeDoubleArray excess;
    final HugeDoubleArray prize;

    GlobalRelabelling(CostFlowGraph costFlowGraph, HugeDoubleArray excess, HugeDoubleArray prize) {
        this.costFlowGraph = costFlowGraph;
        this.excess = excess;
        this.prize = prize;
    }

    void relabelGlobal2(double epsilon) {
        var sSetFrontier = HugeLongArrayQueue.newQueue(costFlowGraph.nodeCount());
        var nodeInSSet = HugeAtomicBitSet.create(costFlowGraph.nodeCount());
        var activeNodesNotFound = new MutableLong(0);
        var pq = HugeLongPriorityQueue.min(costFlowGraph.nodeCount());
        for (long v = 0; v < costFlowGraph.nodeCount(); v++) {
            if (excess.get(v) < -TOLERANCE) {
                nodeInSSet.set(v);
                sSetFrontier.add(v);
            } else if (excess.get(v) > TOLERANCE) {
                activeNodesNotFound.increment();
            }
        }

        AtomicLong epsilonOffset = new AtomicLong();
        while (activeNodesNotFound.longValue() > 0) { //while i do whatever this loop is supposed to do
            while (!sSetFrontier.isEmpty()) {  //this triggers a round of adding nodes reachable by adding  offset * epsilon to every node not in SS
                var v = sSetFrontier.remove();
                costFlowGraph.forEachRelationship(
                    v, (s, t, r, residualCapacity, cost, isReverse) -> {
                        var reverseResidualCapacity = costFlowGraph.reverseResidualCapacity(r, isReverse);
                        if (nodeInSSet.get(t) || reverseResidualCapacity <= TOLERANCE) return true;
                        //let us consider the updated prize for t
                        var actualPrize = prize.get(t) - epsilonOffset.longValue() * epsilon;
                        var reverseReducedCost = (-cost) + actualPrize - prize.get(s);

                        if (reverseReducedCost < 0) {
                            if (!nodeInSSet.getAndSet(t)) {
                                sSetFrontier.add(t); //add to frontier if new
                                prize.set(t, actualPrize); //this guy is added NOW and will be processed NOW
                                if (excess.get(t) > TOLERANCE) {
                                    activeNodesNotFound.decrement();
                                }
                            }
                        } else {
                            //here we want to calculate the amount of epsilon updates t needs to make this hold true
                            //essentially, we compute the time of the event as  "current epsilon updates" + whatever is needed
                            //since s updated with  offset * epsilon itself, the time itself needs correction by summing the offset
                            long diff = (long) (Math.ceil((actualPrize - prize.get(s) - cost) / epsilon) + epsilonOffset.longValue());
                            diff = Math.max(
                                1,
                                diff
                            ); //this is for some weird case where everything is zero :D we still need epsilon update to go <0
                            if (!pq.containsElement(t)) {
                                pq.add(t, diff);
                            } else if (pq.cost(t) > diff) {
                                pq.set(t, diff);
                            }
                        }
                        return true;
                    }
                );
            }

            if (!pq.isEmpty()) { //wooho there are nodes to be added
                while (nodeInSSet.get(pq.top())) {
                    pq.pop(); //removing nods that are all ready in frontier from other operations, they're just noise
                    if (pq.isEmpty()) break;
                }
                if (!pq.isEmpty()) { //if the pq still has elements!
                    long top = pq.top();
                    epsilonOffset.set((long) pq.cost(top)); //the offset becomes the earliest time
                    sSetFrontier.add(top); //add to frontier
                    nodeInSSet.set(top); //AND update bitset you moron

                    //we can technically continue pushing all elements that have .cost == the current cost
                    //but they will anyway be dealt with next iteration dunno
                    var actualPrize = prize.get(top) - epsilonOffset.longValue() * epsilon;
                    prize.set(top, actualPrize);
                    if (excess.get(top) > TOLERANCE) {
                        activeNodesNotFound.decrement();
                    }

                }
            }
        }
        for (long i = 0; i < costFlowGraph.nodeCount(); ++i) {
            if (!nodeInSSet.get(i)) {
                prize.addTo(i, -epsilon * epsilonOffset.longValue());
            }
        }
    }

}
