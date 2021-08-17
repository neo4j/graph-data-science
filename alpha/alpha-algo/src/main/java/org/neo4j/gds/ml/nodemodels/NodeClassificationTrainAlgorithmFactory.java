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
package org.neo4j.gds.ml.nodemodels;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;

import java.util.List;

public class NodeClassificationTrainAlgorithmFactory extends AlgorithmFactory<NodeClassificationTrain, NodeClassificationTrainConfig> {

    @Override
    public NodeClassificationTrain build(
        Graph graph,
        NodeClassificationTrainConfig configuration,
        AllocationTracker tracker,
        ProgressTracker progressTracker
    ) {
        return NodeClassificationTrain.create(graph, configuration, tracker, progressTracker);
    }

    @Override
    public MemoryEstimation memoryEstimation(NodeClassificationTrainConfig configuration) {
        return MemoryEstimations.builder(NodeClassificationTrain.class)
            .add("algorithm", NodeClassificationTrain.estimate(configuration))
            .build();
    }

    @Override
    protected String taskName() {
        return "NCTrain";
    }

    @Override
    public Task progressTask(
        Graph graph, NodeClassificationTrainConfig config
    ) {
        return Tasks.task(
            taskName(),
            Tasks.leaf("ShuffleAndSplit"),
            Tasks.iterativeOpen(
                "SelectBestModel",
                () -> List.of(
                    Tasks.leaf("Train"),
                    Tasks.leaf("Evaluate")
                )
            ),
            Tasks.leaf("SelectModel"),
            Tasks.leaf("TrainSelectedOnRemainder"),
            Tasks.leaf("EvaluateSelectedModel"),
            Tasks.leaf("RetrainSelectedModel")
        );
    }
}
