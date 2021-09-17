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
package org.neo4j.gds.embeddings.node2vec;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.degree.DegreeCentralityFactory;

import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.multiplyExact;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class Node2VecAlgorithmFactory<CONFIG extends Node2VecBaseConfig> extends AlgorithmFactory<Node2Vec, CONFIG> {

    @Override
    protected String taskName() {
        return "Node2Vec";
    }

    @Override
    protected Node2Vec build(
        Graph graph, CONFIG configuration, AllocationTracker allocationTracker, ProgressTracker progressTracker
    ) {
        validateConfig(configuration, graph);
        return new Node2Vec(graph, configuration, progressTracker, allocationTracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return Node2Vec.memoryEstimation(configuration);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {

        var randomWalkTasks = new ArrayList<Task>();
        if (graph.hasRelationshipProperty()) {
            randomWalkTasks.add(DegreeCentralityFactory.degreeCentralityProgressTask(graph));
        }
        randomWalkTasks.add(Tasks.leaf("create walks", graph.nodeCount()));

        return Tasks.task(
            taskName(),
            Tasks.task(
                "RandomWalk",
                randomWalkTasks
            ),
            Tasks.iterativeFixed(
                "train",
                () -> List.of(Tasks.leaf("iteration")),
                config.iterations()
            )
        );
    }

    private void validateConfig(CONFIG config, Graph graph) {
        try {
            var ignored = multiplyExact(
                multiplyExact(graph.nodeCount(), config.walksPerNode()),
                config.walkLength()
            );
        } catch (ArithmeticException ex) {
            throw new IllegalArgumentException(
                formatWithLocale(
                    "Aborting execution, running with the configured parameters is likely to overflow: node count: %d, walks per node: %d, walkLength: %d." +
                    " Try reducing these parameters or run on a smaller graph.",
                    graph.nodeCount(),
                    config.walksPerNode(),
                    config.walkLength()
                ));
        }
    }
}
