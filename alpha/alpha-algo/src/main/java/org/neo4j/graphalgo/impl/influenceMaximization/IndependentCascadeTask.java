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
package org.neo4j.graphalgo.impl.influenceMaximization;

import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArrayStack;
import org.neo4j.graphalgo.core.utils.queue.HugeLongPriorityQueue;

import java.util.Random;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

final class IndependentCascadeTask implements Runnable {
    private static final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Graph graph;
    private final double propagationProbability;
    private final long monteCarloSimulations;
    private final long candidateNode;
    private final long[] seedSetNodes;
    private final HugeLongPriorityQueue spreads;
    private final LongScatterSet active;
    private final HugeLongArrayStack newActive;
    private final Random rand;
    private double spread;

    public IndependentCascadeTask(
        Graph graph,
        double propagationProbability,
        long monteCarloSimulations,
        long candidateNode,
        long[] seedSetNodes,
        HugeLongPriorityQueue spreads,
        AllocationTracker tracker
    ) {
        this.graph = graph.concurrentCopy();

        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;
        this.candidateNode = candidateNode;
        this.seedSetNodes = seedSetNodes;
        this.spreads = spreads;

        this.active = new LongScatterSet();
        this.newActive = HugeLongArrayStack.newStack(graph.nodeCount(), tracker);

        this.rand = new Random();
    }

    @Override
    public void run() {
        //Loop over the Monte-Carlo simulations
        spread = 0;
        for (long i = 0; i < monteCarloSimulations; i++) {
            initStructures();
            spread += newActive.size();
            //For each newly active node, find its neighbors that become activated
            while (!newActive.isEmpty()) {
                //Determine neighbors that become infected
                rand.setSeed(i);
                long node = newActive.pop();
                graph.forEachRelationship(node, (source, target) ->
                {
                    if (rand.nextDouble() < propagationProbability) {
                        spread++;
                        if (!active.contains(target)) {
                            //Add newly activated nodes to the set of activated nodes
                            newActive.push(target);
                            active.add(target);
                        }
                    }
                    return true;
                });
            }
        }
        lock.writeLock().lock();
        try {
            spreads.add(candidateNode, (double) Math.round((spread /= monteCarloSimulations) * 1000) / 1000);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void initStructures() {
        active.clear();
        active.add(candidateNode);
        active.addAll(seedSetNodes);

        newActive.push(candidateNode);
        for (long kNode : seedSetNodes) {
            newActive.push(kNode);
        }
    }
}
