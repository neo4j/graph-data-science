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

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

public class CELFAlgorithmFactory<CONFIG extends InfluenceMaximizationBaseConfig> extends GraphAlgorithmFactory<CELF, CONFIG> {

    @Override
    public String taskName() {
        return "CELF";
    }

    public CELF build(
        Graph graph,
        CELFParameters celfParameters,
        ProgressTracker progressTracker
    ) {
        return new CELF(graph, celfParameters, DefaultPool.INSTANCE, progressTracker);
    }

    @Override
    public CELF build(
        Graph graph,
        CONFIG configuration,
        ProgressTracker progressTracker
    ) {
        var parameters = configuration.toParameters();
        return build(graph, parameters, progressTracker);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return Tasks.task(
            "CELF",
            Tasks.leaf("Greedy", graph.nodeCount()),
            Tasks.leaf("LazyForwarding", config.seedSetSize() - 1)
        );
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return new CELFMemoryEstimateDefinition(configuration.toParameters()).memoryEstimation();
    }

}
