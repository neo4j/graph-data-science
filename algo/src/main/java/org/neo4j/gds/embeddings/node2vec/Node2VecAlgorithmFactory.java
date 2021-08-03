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
import org.neo4j.gds.degree.DegreeCentralityFactory;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.core.utils.BatchingProgressLogger;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.progress.ProgressEventTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Task;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.TaskProgressTracker;
import org.neo4j.graphalgo.core.utils.progress.v2.tasks.Tasks;
import org.neo4j.logging.Log;

import java.util.List;

import static java.lang.Math.multiplyExact;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class Node2VecAlgorithmFactory<CONFIG extends Node2VecBaseConfig> implements AlgorithmFactory<Node2Vec, CONFIG> {
    @Override
    public Node2Vec build(
        Graph graph,
        CONFIG configuration,
        AllocationTracker tracker,
        Log log,
        ProgressEventTracker eventTracker
    ) {
        var progressLogger = new BatchingProgressLogger(
            log,
            0, //dummy value, gets overridden
            "Node2Vec",
            configuration.concurrency(),
            eventTracker
        );
        validateConfig(configuration, graph);
        var progressTracker = new TaskProgressTracker(progressTask(graph, configuration), progressLogger);
        return new Node2Vec(graph, configuration, progressTracker, tracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(CONFIG configuration) {
        return Node2Vec.memoryEstimation(configuration);
    }

    @Override
    public Task progressTask(Graph graph, CONFIG config) {
        var randomWalkTask = graph.hasRelationshipProperty()
            ? Tasks.task("RandomWalk", DegreeCentralityFactory.degreeCentralityProgressTask(graph))
            : Tasks.leaf("RandomWalk");
        return Tasks.task(
            "compute",
            randomWalkTask,
            Tasks.iterativeFixed(
                "train",
                () -> List.of(Tasks.leaf("produce positive samples")),
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
