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
package org.neo4j.gds.influenceMaximization;

import com.carrotsearch.hppc.BitSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.partition.Partition;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;

import java.util.SplittableRandom;

final class ICInitTask implements Runnable {

    private final Graph localGraph;
    private final double propagationProbability;
    private final int monteCarloSimulations;

    private final HugeDoubleArray singleSpreadArray;

    private final Partition partition;

    private final BitSet active;

    private final HugeLongArrayStack newActive;
    private final long initialRandomSeed;

    private final ProgressTracker progressTracker;

    ICInitTask(
        Partition partition,
        Graph graph,
        double propagationProbability,
        int monteCarloSimulations,
        HugeDoubleArray singleSpreadArray,
        long initialRandomSeed,
        ProgressTracker progressTracker
    ) {

        this.partition = partition;
        this.localGraph = graph.concurrentCopy();
        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;
        this.singleSpreadArray = singleSpreadArray;
        this.progressTracker = progressTracker;
        active = new BitSet(graph.nodeCount());
        newActive = HugeLongArrayStack.newStack(graph.nodeCount());

        this.initialRandomSeed = initialRandomSeed;
    }

    private void initDataStructures(long candidateNodeId) {
        active.clear();
        newActive.push(candidateNodeId);
        active.set(candidateNodeId);
    }

    public void run() {
        //Loop over the Monte-Carlo simulations

        long startNode = partition.startNode();

        long endNode = startNode + partition.nodeCount();
       
        for (long nodeId = startNode; nodeId < endNode; ++nodeId) {

            double nodeSpread = 0d;
            for (int simulation = 0; simulation < monteCarloSimulations; ++simulation) {
                initDataStructures(nodeId);
                //For each newly active node, find its neighbors that become activated
                while (!newActive.isEmpty()) {
                    //Determine neighbors that become infected
                    long nextExaminedNode = newActive.pop();
                    var rand = new SplittableRandom(initialRandomSeed + simulation);

                    localGraph.forEachRelationship(nextExaminedNode, (source, target) ->
                    {
                        if (rand.nextDouble() < propagationProbability) {
                            if (!active.get(target)) {
                                //Add newly activated nodes to the set of activated nodes
                                newActive.push(target);
                                active.set(target);
                            }
                        }
                        return true;
                    });
                }
                nodeSpread += active.cardinality();
            }
            singleSpreadArray.set(nodeId, nodeSpread / monteCarloSimulations);
            progressTracker.logProgress();

        }

        }

    }
