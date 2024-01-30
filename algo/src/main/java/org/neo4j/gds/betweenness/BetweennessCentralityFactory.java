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
package org.neo4j.gds.betweenness;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.Optional;

public class BetweennessCentralityFactory<CONFIG extends BetweennessCentralityBaseConfig> extends GraphAlgorithmFactory<BetweennessCentrality, CONFIG> {

    @Override
    public String taskName() {
        return "BetweennessCentrality";
    }

    public BetweennessCentrality build(
        Graph graph,
        BetweennessCentralityParameters parameters,
        ProgressTracker progressTracker
    ) {
        var samplingSize = parameters.samplingSize();
        var samplingSeed = parameters.samplingSeed();

        var strategy = samplingSize.isPresent() && samplingSize.get() < graph.nodeCount()
            ? new RandomDegreeSelectionStrategy(samplingSize.get(), samplingSeed)
            : new FullSelectionStrategy();

        ForwardTraverser.Factory traverserFactory = parameters.hasRelationshipWeightProperty()
            ? ForwardTraverser.Factory.weighted()
            : ForwardTraverser.Factory.unweighted();

        return new BetweennessCentrality(
            graph,
            strategy,
            traverserFactory,
            DefaultPool.INSTANCE,
            parameters.concurrency(),
            progressTracker
        );
    }

    @Override
    public BetweennessCentrality build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {
        return build(graph, configuration.toParameters(), progressTracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return new BetweennessCentralityMemoryEstimateDefinition().memoryEstimation(configuration);
    }

    public Task progressTask(Graph graph, Optional<Long> samplingSize) {
        return Tasks.leaf(taskName(), samplingSize.orElse(graph.nodeCount()));
    }
    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return progressTask(graph, config.samplingSize());
    }
}
