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

import com.carrotsearch.hppc.LongScatterSet;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeLongArrayStack;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

import java.util.SplittableRandom;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

final class IndependentCascade {
    private static final Lock lock = new ReentrantLock();

    private final Graph graph;
    private final double propagationProbability;
    private final long monteCarloSimulations;
    private final HugeLongPriorityQueue spreads;
    private final LongScatterSet active;
    private final HugeLongArrayStack newActive;
    private double spread;

    IndependentCascade(
        Graph graph,
        double propagationProbability,
        long monteCarloSimulations,
        HugeLongPriorityQueue spreads
    ) {
        this.graph = graph.concurrentCopy();
        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;
        this.spreads = spreads;

        this.active = new LongScatterSet();
        this.newActive = HugeLongArrayStack.newStack(graph.nodeCount());

    }

    public void run(long candidateNode, long[] seedSetNodes) {
        //Loop over the Monte-Carlo simulations
        spread = 0;
        for (long i = 0; i < monteCarloSimulations; i++) {
            initStructures(candidateNode, seedSetNodes);
            spread += newActive.size();
            //For each newly active node, find its neighbors that become activated
            while (!newActive.isEmpty()) {
                //Determine neighbors that become infected
                SplittableRandom rand = new SplittableRandom(i);
                long node = newActive.pop();
                graph.forEachRelationship(node, (source, target) ->
                {
                    if (rand.nextDouble() < propagationProbability) {
                        if (!active.contains(target)) {
                            //Add newly activated nodes to the set of activated nodes
                            spread++;
                            newActive.push(target);
                            active.add(target);
                        }
                    }
                    return true;
                });
            }
        }
        spread /= monteCarloSimulations;
        addCandidateNode(candidateNode);
    }

    private void addCandidateNode(long candidateNode) {
        try {
            lock.lock();
            spreads.add(candidateNode, (double) Math.round(spread * 1000) / 1000);
        } finally {
            lock.unlock();
        }
    }

    private void initStructures(long candidateNode, long[] seedSetNodes) {
        active.clear();
        active.add(candidateNode);
        active.addAll(seedSetNodes);

        newActive.push(candidateNode);
        for (long kNode : seedSetNodes) {
            newActive.push(kNode);
        }
    }
}
