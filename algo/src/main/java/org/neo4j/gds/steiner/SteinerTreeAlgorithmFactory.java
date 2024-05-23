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
package org.neo4j.gds.steiner;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.ArrayList;
import java.util.stream.Collectors;

public class SteinerTreeAlgorithmFactory<CONFIG extends SteinerTreeBaseConfig> extends GraphAlgorithmFactory<ShortestPathsSteinerAlgorithm, CONFIG> {

    public ShortestPathsSteinerAlgorithm build(
        Graph graph,
        SteinerTreeParameters parameters,
        ProgressTracker progressTracker
    ) {
        var mappedTargetNodes = parameters.targetNodes().stream()
            .map(graph::safeToMappedNodeId)
            .collect(Collectors.toList());
        return new ShortestPathsSteinerAlgorithm(
            graph,
            graph.toMappedNodeId(parameters.sourceNode()),
            mappedTargetNodes,
            parameters.delta(),
            parameters.concurrency(),
            parameters.applyRerouting(),
            DefaultPool.INSTANCE,
            progressTracker,
            TerminationFlag.RUNNING_TRUE
        );
    }

    @Override
    public ShortestPathsSteinerAlgorithm build(Graph graph, CONFIG configuration, ProgressTracker progressTracker) {
        return build(graph, configuration.toParameters(), progressTracker);
    }

    @Override
    public String taskName() {
        return "SteinerTree";
    }

    public Task progressTask(Graph graph, int targetNodesSize, boolean applyRerouting) {
        var subtasks = new ArrayList<Task>();
        subtasks.add(Tasks.leaf("Traverse", targetNodesSize));
        if (applyRerouting) {
            long nodeCount = graph.nodeCount();
            subtasks.add(Tasks.leaf("Reroute", nodeCount));
        }
        return Tasks.task(taskName(), subtasks);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        return progressTask(graph, config.targetNodes().size(), config.applyRerouting());
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG config) {
        return new SteinerTreeMemoryEstimateDefinition(config.applyRerouting()).memoryEstimation();
    }
}
