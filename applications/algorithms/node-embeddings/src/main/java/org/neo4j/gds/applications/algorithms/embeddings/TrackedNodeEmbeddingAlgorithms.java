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
package org.neo4j.gds.applications.algorithms.embeddings;

import org.neo4j.gds.NodeEmbeddingsAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerManager;
import org.neo4j.gds.embeddings.fastrp.FastRPBaseConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPConfigTransformer;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.hashgnn.HashGNNConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNConfigTransformer;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;

class TrackedNodeEmbeddingAlgorithms {
    private final ProgressTrackerManager progressTrackerManager = new ProgressTrackerManager();

    private final NodeEmbeddingAlgorithms algorithms;
    private final ProgressTrackerCreator progressTrackerCreator;

    TrackedNodeEmbeddingAlgorithms(NodeEmbeddingAlgorithms algorithms, ProgressTrackerCreator progressTrackerCreator) {
        this.algorithms = algorithms;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    FastRPResult fastRP(Graph graph, FastRPBaseConfig configuration) {
        var params = FastRPConfigTransformer.toParameters(configuration);

        var task = NodeEmbeddingsAlgorithmTasks.fastRP(graph, params);
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.fastRP(graph, params, progressTracker),
            progressTracker,
            true
        );
    }

    HashGNNResult hashGnn(Graph graph, HashGNNConfig configuration) {
        var params = HashGNNConfigTransformer.toParameters(configuration);

        var task = NodeEmbeddingsAlgorithmTasks.hashGNN(graph, params, configuration.relationshipTypes());
        var progressTracker = progressTrackerCreator.create(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> algorithms.hashGnn(graph, params, progressTracker),
            progressTracker,
            true
        );
    }
}
