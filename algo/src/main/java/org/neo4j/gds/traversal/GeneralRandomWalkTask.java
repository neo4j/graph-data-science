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
package org.neo4j.gds.traversal;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.ml.core.samplers.RandomWalkSampler;

import java.util.function.Function;

public class GeneralRandomWalkTask implements Runnable {

    protected final Graph graph;
    protected final NextNodeSupplier nextNodeSupplier;
    protected final ProgressTracker progressTracker;
    protected final RandomWalkBaseConfig config;
    protected final RandomWalkSampler sampler;
    protected Function<long[], Boolean> pathConsumer;


    public GeneralRandomWalkTask(
        NextNodeSupplier nextNodeSupplier,
        RandomWalkSampler.CumulativeWeightSupplier cumulativeWeightSupplier,
        RandomWalkBaseConfig config,
        Graph graph,
        long randomSeed,
        ProgressTracker progressTracker
    ) {

        var maxProbability = Math.max(Math.max(1 / config.returnFactor(), 1.0), 1 / config.inOutFactor());
        var normalizedReturnProbability = (1 / config.returnFactor()) / maxProbability;
        var normalizedSameDistanceProbability = 1 / maxProbability;
        var normalizedInOutProbability = (1 / config.inOutFactor()) / maxProbability;

        this.nextNodeSupplier = nextNodeSupplier;
        this.graph = graph;
        this.config = config;
        this.progressTracker = progressTracker;
        this.sampler = new RandomWalkSampler(
            cumulativeWeightSupplier,
            config.walkLength(),
            normalizedReturnProbability,
            normalizedSameDistanceProbability,
            normalizedInOutProbability,
            graph,
            randomSeed
        );

    }

    public void withPathConsumer(Function<long[], Boolean> pathConsumer) {
        this.pathConsumer = pathConsumer;
    }
    @Override
    public void run() {
        long nodeId;

        while (true) {
            nodeId = nextNodeSupplier.nextNode();

            if (nodeId == NextNodeSupplier.NO_MORE_NODES) break;

            if (graph.degree(nodeId) == 0) {
                progressTracker.logProgress();
                continue;
            }
            var walksPerNode = config.walksPerNode();

            sampler.prepareForNewNode(nodeId);

            for (int walkIndex = 0; walkIndex < walksPerNode; walkIndex++) {
                var path= sampler.walk(nodeId);
                   boolean shouldContinue= pathConsumer.apply(path);
                    if (!shouldContinue){
                        break;
                    }
            }

            progressTracker.logProgress();
        }

    }

}
