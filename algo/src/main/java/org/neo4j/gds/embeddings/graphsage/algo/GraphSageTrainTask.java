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
package org.neo4j.gds.embeddings.graphsage.algo;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.embeddings.graphsage.GraphSageModelTrainer;
import org.neo4j.gds.embeddings.graphsage.TrainConfigTransformer;

public final class GraphSageTrainTask {
    private GraphSageTrainTask() {}

    public static Task create(IdMap idMap, GraphSageTrainConfig configuration) {
        var parameters = TrainConfigTransformer.toParameters(configuration);

        return Tasks.task(
            AlgorithmLabel.GraphSageTrain.asString(),
            GraphSageModelTrainer.progressTasks(
                parameters.numberOfBatches(idMap.nodeCount()),
                parameters.batchesPerIteration(idMap.nodeCount()),
                parameters.maxIterations(),
                parameters.epochs()
            )
        );
    }
}
