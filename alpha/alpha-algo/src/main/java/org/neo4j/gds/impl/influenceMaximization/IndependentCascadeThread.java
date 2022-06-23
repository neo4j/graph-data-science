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
import org.neo4j.gds.core.utils.partition.Partition;

import java.util.SplittableRandom;

final class IndependentCascadeThread implements  Runnable{

    private final long[] seedSetNodes;
    private int seedNodeCounter;

    private Partition partition;
    private long candidateNodeId;

    private double localSpread;

    private final Graph localGraph;
    private final BitSet active;

    private final BitSet newActive;

    private final double propagationProbability;

    public IndependentCascadeThread(
        Partition partition,
        Graph graph,
        long[] seedSetNodes,
        double propagationProbability
    ) {
        this.localGraph = graph.concurrentCopy();
        active = new BitSet(graph.nodeCount());
        newActive = new BitSet(graph.nodeCount());
        this.propagationProbability = propagationProbability;
        this.seedSetNodes = seedSetNodes;
        seedNodeCounter = 1;
        this.partition = partition;
    }

    public void incrementSeedNode() {
        seedNodeCounter++;
    }


    public void setCandidateNodeId(long candidateNodeId) {
        this.candidateNodeId = candidateNodeId;
    }

    public double getSpread() {
        return localSpread;
    }

    public void run() {
        //Loop over the Monte-Carlo simulations
        localSpread = 0;
        int startingSeed = (int) partition.startNode();
        int endingSeed = (int) partition.nodeCount() + startingSeed;

        for (long seed = startingSeed; seed < endingSeed; seed++) {
            newActive.clear();
            newActive.set(candidateNodeId);
            active.set(candidateNodeId);
            for (int i = 0; i < seedNodeCounter; ++i) {
                newActive.set(seedSetNodes[i]);
                active.set(seedSetNodes[i]);
            }
            SplittableRandom rand = new SplittableRandom(seed);
            localSpread += newActive.cardinality();
            //For each newly active node, find its neighbors that become activated
            while (!newActive.isEmpty()) {
                //Determine neighbors that become infected
                long nodeId = newActive.nextSetBit(0);
                newActive.flip(nodeId);
                localGraph.forEachRelationship(nodeId, (source, target) ->
                {
                    if (rand.nextDouble() < propagationProbability) {
                        if (!active.get(target)) {
                            //Add newly activated nodes to the set of activated nodes
                            localSpread++;
                            newActive.set(target);
                            active.set(target);
                        }
                    }
                    return true;
                });
            }
        }
    }



}
