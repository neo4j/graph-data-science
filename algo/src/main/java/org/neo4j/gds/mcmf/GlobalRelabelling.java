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
import org.apache.commons.lang3.mutable.MutableLong;
import org.neo4j.gds.collections.ha.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayQueue;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

import static org.neo4j.gds.mcmf.MinCostFunctions.TOLERANCE;
import static org.neo4j.gds.mcmf.MinCostFunctions.isAdmissible;
import static org.neo4j.gds.mcmf.MinCostFunctions.isResidualEdge;
import static org.neo4j.gds.mcmf.MinCostFunctions.reducedCost;

 class GlobalRelabelling {
   private final CostFlowGraph costFlowGraph;
   private final HugeDoubleArray excess;
   private final HugeDoubleArray prize;
   private final HugeLongArrayQueue frontier;
   private final BitSet nodeInSet;
   private final HugeLongPriorityQueue pq;

    GlobalRelabelling(CostFlowGraph costFlowGraph, HugeDoubleArray excess, HugeDoubleArray prize) {
        this.costFlowGraph = costFlowGraph;
        this.excess = excess;
        this.prize = prize;
        this.frontier = HugeLongArrayQueue.newQueue(costFlowGraph.nodeCount());
        this.nodeInSet = new BitSet(costFlowGraph.nodeCount());
        this.pq = HugeLongPriorityQueue.min(costFlowGraph.nodeCount());
    }

    void relabellingWithPriorityQueue(double epsilon) {
        nodeInSet.clear();
        frontier.clear();

        var activeNodesNotFound = new MutableLong(0);
        for (long v = 0; v < costFlowGraph.nodeCount(); v++) {
            if (excess.get(v) < -TOLERANCE) {
                addToFrontier(v);
            } else if (excess.get(v) > TOLERANCE) {
                activeNodesNotFound.increment();
            }
        }

        MutableDouble epsilonOffset = new MutableDouble();
        while (activeNodesNotFound.longValue() > 0) { //while i do whatever this loop is supposed to do
            exhaustFrontier(epsilonOffset.doubleValue(),epsilon,activeNodesNotFound);
            if (activeNodesNotFound.longValue() == 0) {
                break;
            }
            double newOffset = extractFromPriorityQueue(epsilon,activeNodesNotFound);
            epsilonOffset.setValue(newOffset);
        }
        for (long i = 0; i < costFlowGraph.nodeCount(); ++i) {
            if (!nodeInSet.get(i)) {
                prize.addTo(i, -epsilon * epsilonOffset.longValue());
            }
        }
    }

    double extractFromPriorityQueue(double epsilon,MutableLong activeNodesNotFound){
        if (!pq.isEmpty()) { //there are nodes to be added
            while (nodeInSet.get(pq.top())) {
                pq.pop(); //removing nods that are all ready in frontier from other operations, they're just noise
                if (pq.isEmpty()) break;
            }
            if (!pq.isEmpty()) { //if the pq still has elements!
                long top = pq.top();
                double epsilonOffset = pq.cost(top);
                addToFrontier(top);

                //we can technically continue pushing all elements that have .cost == the current cost
                //but they will anyway be dealt with next iteration anyway
                var actualPrize = prize.get(top) - epsilonOffset * epsilon;
                prize.set(top, actualPrize);
                checkIfActiveNode(top,activeNodesNotFound);
                return epsilonOffset;
            }
        }
        throw new RuntimeException("Should never be empty");

    }
     void addToFrontier(long node){
         frontier.add(node); //add to frontier
         nodeInSet.set(node); //AND update bitset you
     }
     void exhaustFrontier(double offset, double epsilon, MutableLong activeNodesNotFound){
        while (!frontier.isEmpty() & activeNodesNotFound.longValue() > 0) {  //this triggers a round of adding nodes reachable by adding  offset * epsilon to every node not in SS
            var v = frontier.remove();
            traverseNode(v,offset,epsilon,activeNodesNotFound);
        }
    }

    private void checkIfActiveNode(long node, MutableLong activeNodesNotFound){
        if (excess.get(node) > TOLERANCE) {
            activeNodesNotFound.decrement();
        }
    }

    void traverseNode(long node,double offset, double epsilon, MutableLong activeNodesNotFound){
        costFlowGraph.forEachRelationship(
            node, (s, t, r, residualCapacity, cost, isReverse) -> {
                var reverseResidualCapacity = costFlowGraph.reverseResidualCapacity(r, isReverse);
                if (nodeInSet.get(t) || !isResidualEdge(reverseResidualCapacity)) return true;
                //let us consider the updated prize for t
                var actualPrize = prize.get(t) - offset * epsilon;
                var reverseReducedCost = reducedCost(-cost,actualPrize,prize.get(s));
                if (isAdmissible(reverseReducedCost)) {
                    if (!nodeInSet.get(t)) {
                        addToFrontier(t);
                        prize.set(t, actualPrize); //this guy is added NOW and will be processed NOW
                        checkIfActiveNode(t,activeNodesNotFound);
                    }
                } else {
                    var diff = computeEventTime(actualPrize, prize.get(s), cost, epsilon, offset);
                    queuePush(t,diff);
                }
                return true;
            }
        );
    }

    double computeEventTime(double tActualPrize, double selfPrize, double relCost, double epsilon, double offset){
        //here we want to calculate the amount of epsilon updates t needs to make this hold true
        //essentially, we compute the time of the event as  "current epsilon updates" + whatever is needed
        //since s updated with  offset * epsilon itself, the time itself needs correction by summing the offset
        //this is for some weird case where everything is zero :D we still need epsilon update to go <0
        var diff = (long) (Math.ceil((tActualPrize - selfPrize - relCost) / epsilon) + offset);
        return Math.max(
            1,
            diff
        );
    }
    private void queuePush(long node, double diff){
        if (!pq.containsElement(node)) {
            pq.add(node, diff);
        } else if (pq.cost(node) > diff) {
            pq.set(node, diff);
        }
    }

}
