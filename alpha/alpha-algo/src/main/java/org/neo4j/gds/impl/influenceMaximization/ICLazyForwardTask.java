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

final class ICLazyForwardTask implements Runnable {

    private final long[] seedSetNodes;
    private final long initialRandomSeed;
    private int seedNodeCounter;

    private final Partition partition;
    private final long[] candidateNodeIds;

    private int candidateSetSizes;

    private final double[] localSpread;

    private final Graph localGraph;
    private final BitSet candidateActive;

    private final BitSet seedActive;

    private final HugeLongArrayStack newActive;

    private final double propagationProbability;

    ICLazyForwardTask(
        Partition partition,
        Graph graph,
        long[] seedSetNodes,
        double propagationProbability,
        long initialRandomSeed,
        int batchSize
    ) {
        this.localGraph = graph;
        this.newActive = HugeLongArrayStack.newStack(graph.nodeCount());
        this.seedActive = new BitSet(graph.nodeCount());
        this.candidateActive = new BitSet(graph.nodeCount());
        this.propagationProbability = propagationProbability;
        this.seedSetNodes = seedSetNodes;
        this.initialRandomSeed = initialRandomSeed;
        this.seedNodeCounter = 1;
        this.partition = partition;
        this.localSpread = new double[batchSize];
        this.candidateNodeIds = new long[batchSize];
    }

    void incrementSeedNode(long newSetNode) {
        seedSetNodes[this.seedNodeCounter++] = newSetNode;
    }

    void setCandidateNodeId(long[] candidateNodeIds, int candidateSetSize) {
        if (candidateSetSize >= 0) {
            System.arraycopy(candidateNodeIds, 0, this.candidateNodeIds, 0, candidateSetSize);
        }
        this.candidateSetSizes = candidateSetSize;
    }

    double getSpread(int j) {
        return localSpread[j];
    }

    private void initCandidate(long candidateId) {
        if (!seedActive.get(candidateId)) {
            candidateActive.clear();
            candidateActive.set(candidateId);
            newActive.push(candidateId);
        }
    }

    private void initDataStructures() {
        seedActive.clear();

        for (int i = 0; i < seedNodeCounter; ++i) {
            newActive.push(seedSetNodes[i]);
            seedActive.set(seedSetNodes[i]);
        }
    }

    private void seedTraverse(long seed) {
        while (!newActive.isEmpty()) {
            //Determine neighbors that become infected
            long nodeId = newActive.pop();
            SplittableRandom rand = new SplittableRandom(initialRandomSeed + seed);
            localGraph.forEachRelationship(nodeId, (source, target) ->
            {
                if (rand.nextDouble() < propagationProbability) {
                    if (!seedActive.get(target)) {
                        //Add newly activated nodes to the set of activated nodes
                        newActive.push(target);
                        seedActive.set(target);
                    }
                }
                return true;
            });
        }
    }


    private void candidateTraverse(long seed) {
        while (!newActive.isEmpty()) {
            //Determine neighbors that become infected
            long nodeId = newActive.pop();
            SplittableRandom rand = new SplittableRandom(initialRandomSeed + seed);
            localGraph.forEachRelationship(nodeId, (source, target) ->
            {
                if (rand.nextDouble() < propagationProbability) {
                    if (!seedActive.get(target) && !candidateActive.get(target)) {
                        //Add newly activated nodes to the set of activated nodes
                        newActive.push(target);
                        candidateActive.set(target);
                    }
                }
                return true;
            });
        }
    }

    public void run() {
        //Loop over the Monte-Carlo simulations
        for (int j = 0; j < candidateSetSizes; ++j) {
            localSpread[j] = 0;
        }
        long startingSeed = partition.startNode();
        long endingSeed = partition.nodeCount() + startingSeed;

        for (long seed = startingSeed; seed < endingSeed; seed++) {
            initDataStructures();
            seedTraverse(seed);
            for (int j = 0; j < candidateSetSizes; ++j) {
                initCandidate(candidateNodeIds[j]);
                localSpread[j] += seedActive.cardinality();
                if (!newActive.isEmpty()) {
                    candidateTraverse(seed);
                    localSpread[j] += candidateActive.cardinality();
                }
            }

        }
    }

}
