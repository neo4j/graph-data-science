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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.RunWithConcurrency;
import org.neo4j.gds.core.utils.partition.PartitionUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

final class ICLazyForwardMC {


    private final int concurrency;

    private final List<ICLazyForwardTask> tasks;

    private final ExecutorService executorService;

    public static int DEFAULT_BATCH_SIZE = 10;

    private double spread[];

    static ICLazyForwardMC create(
        Graph graph,
        double propagationProbability,
        int monteCarloSimulations,
        long[] seedSetNodes,
        int concurrency,
        ExecutorService executorService,
        long initialRandomSeed
    ) {
        double[] spread = new double[seedSetNodes.length];

        var tasks = PartitionUtils.rangePartition(
            concurrency,
            monteCarloSimulations,
            partition -> new ICLazyForwardTask(
                partition,
                graph.concurrentCopy(),
                seedSetNodes.clone(),
                propagationProbability,
                initialRandomSeed,
                DEFAULT_BATCH_SIZE
            ),
            Optional.of(monteCarloSimulations / concurrency)
        );
        return new ICLazyForwardMC(tasks, concurrency, executorService, spread);
    }

    ICLazyForwardMC(
        List<ICLazyForwardTask> tasks,
        int concurrency,
        ExecutorService executorService,
        double[] spread
    ) {
//
        this.tasks = tasks;
        this.spread = spread;
        this.concurrency = concurrency;
        this.executorService = executorService;
    }

    public void runForCandidate(long[] candidateIdNodes, int candidateSize) {
        for (var task : tasks) {
            task.setCandidateNodeId(candidateIdNodes, candidateSize);
        }
        for (int j = 0; j < candidateSize; ++j) {
            spread[j] = 0;
        }
        RunWithConcurrency.builder()
            .concurrency(concurrency)
            .tasks(tasks)
            .executor(executorService)
            .run();
        for (var task : tasks) {
            for (int j = 0; j < candidateSize; ++j) {
                spread[j] += task.getSpread(j);
            }
        }
        return;
    }

    public double getSpread(int j) {
        return spread[j];
    }

    void incrementSeedNode(long newSetNode) {
        for (var task : tasks) {
            task.incrementSeedNode(newSetNode);
        }
    }

}
