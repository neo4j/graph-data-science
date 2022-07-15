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
package org.neo4j.gds.impl.influenceMaximization;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.partition.Partition;

import java.util.SplittableRandom;

final class ICLazyForwardThread implements Runnable {

    private final long[] seedSetNodes;
    private long randomSeedStart;
    private int seedNodeCounter;

    private final Partition partition;
    private long candidateNodeId;

    private double localSpread;

    private final Graph localGraph;
    private final BitSet active;

    private final HugeLongArrayStack newActive;

    private final double propagationProbability;

    ICLazyForwardThread(
        Partition partition,
        Graph graph,
        long[] seedSetNodes,
        double propagationProbability,
        long randomSeedStart
    ) {
        this.localGraph = graph;
        this.active = new BitSet(graph.nodeCount());
        this.newActive = HugeLongArrayStack.newStack(graph.nodeCount());
        this.propagationProbability = propagationProbability;
        this.seedSetNodes = seedSetNodes;
        this.randomSeedStart = randomSeedStart;
        this.seedNodeCounter = 1;
        this.partition = partition;
    }

    void incrementSeedNode(long newSetNode) {
        seedSetNodes[this.seedNodeCounter++] = newSetNode;
    }

    void setCandidateNodeId(long candidateNodeId) {
        this.candidateNodeId = candidateNodeId;
    }

    public double getSpread() {
        return this.localSpread;
    }


    private void initDataStructures() {
        active.clear();
        newActive.push(candidateNodeId);
        active.set(candidateNodeId);
        System.out.print("[");
        for (int i = 0; i < seedNodeCounter; ++i) {
            System.out.print(seedSetNodes[i] + " ");
            newActive.push(seedSetNodes[i]);
            active.set(seedSetNodes[i]);
        }
        System.out.println("]");
    }

    public void run() {
        //Loop over the Monte-Carlo simulations
        localSpread = 0;
        int startingSeed = (int) partition.startNode();
        int endingSeed = (int) partition.nodeCount() + startingSeed;

        for (long seed = startingSeed; seed < endingSeed; seed++) {
            initDataStructures();
            System.out.println(seed + " " + startingSeed + " " + endingSeed);
            localSpread += newActive.size();
            //For each newly active node, find its neighbors that become activated
            while (!newActive.isEmpty()) {
                //Determine neighbors that become infected
                long nodeId = newActive.pop();
                SplittableRandom rand = new SplittableRandom(seed + randomSeedStart);
                long finalSeed = seed;
                localGraph.forEachRelationship(nodeId, (source, target) ->
                {
                    double prob = rand.nextDouble();
                    if (prob < propagationProbability) {
                        if (!active.get(target)) {
                            System.out.println("[trial: " + finalSeed + "] " + nodeId + " enables " + target + " by getting " + prob);
                            //Add newly activated nodes to the set of activated nodes
                            localSpread++;
                            newActive.push(target);
                            active.set(target);
                        }
                    }
                    return true;
                });
            }

            //localSpread -= seedNodeCounter;
        }
    }
}
