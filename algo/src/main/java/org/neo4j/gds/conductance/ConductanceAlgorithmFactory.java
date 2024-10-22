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
package org.neo4j.gds.conductance;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import static org.neo4j.gds.conductance.ConductanceConfigTransformer.toParameters;

public class ConductanceAlgorithmFactory<CONFIG extends ConductanceBaseConfig> extends GraphAlgorithmFactory<Conductance, CONFIG> {

    public ConductanceAlgorithmFactory() {
        super();
    }

    @Override
    public String taskName() {
        return "Conductance";
    }

    public Conductance build(Graph graph, ConductanceParameters parameters, ProgressTracker progressTracker) {
        return new Conductance(
            graph,
            parameters.concurrency(),
            parameters.minBatchSize(),
            parameters.hasRelationshipWeightProperty(),
            parameters.communityProperty(),
            DefaultPool.INSTANCE,
            progressTracker
        );
    }

    @Override
    public Conductance build(
        Graph graph,
        CONFIG config,
        ProgressTracker progressTracker
    ) {
        return build(graph, toParameters(config), progressTracker);
    }
    public Task progressTask(long nodeCount) {
        return Tasks.task(
            taskName(),
            Tasks.leaf("count relationships", nodeCount),
            Tasks.leaf("accumulate counts"),
            Tasks.leaf("perform conductance computations")
        );
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return progressTask(graph.nodeCount());
    }
}
