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

import org.bouncycastle.util.Arrays;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;

import java.util.concurrent.atomic.AtomicLong;

class IndependentCascadeRunner implements Runnable {
    private final Graph graph;
    private final HugeLongPriorityQueue spreads;
    private final AtomicLong globalNodeProgress;
    private final double propagationProbability;
    private final int monteCarloSimulations;
    private long[] seedSetNodes;

    IndependentCascadeRunner(
        Graph graph,
        HugeLongPriorityQueue spreads,
        AtomicLong globalNodeProgress,
        double propagationProbability,
        int monteCarloSimulations
    ) {
        this.graph = graph;
        this.spreads = spreads;
        this.globalNodeProgress = globalNodeProgress;
        this.propagationProbability = propagationProbability;
        this.monteCarloSimulations = monteCarloSimulations;
    }

    void setSeedSetNodes(long[] seedSetNodes) {
        this.seedSetNodes = seedSetNodes;
    }

    @Override
    public void run() {
        var independentCascade = new IndependentCascade(
            graph,
            propagationProbability,
            monteCarloSimulations,
            spreads
        );

        var candidateNode = 0L;

        while ((candidateNode = globalNodeProgress.getAndIncrement()) < graph.nodeCount()) {
            if (Arrays.contains(seedSetNodes, candidateNode)) {
                continue;
            }

            independentCascade.run(candidateNode, seedSetNodes);
        }
    }
}
