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
package org.neo4j.gds.applications.algorithms.similarity;

import org.neo4j.gds.SimilarityAlgorithmTasks;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerManager;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

public class TrackedSimilarityAlgorithms {
    private final ProgressTrackerManager progressTrackerManager = new ProgressTrackerManager();

    private final SimilarityAlgorithms similarityAlgorithms;
    private final ProgressTrackerCreator progressTrackerCreator;

    public TrackedSimilarityAlgorithms(
        SimilarityAlgorithms similarityAlgorithms,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.similarityAlgorithms = similarityAlgorithms;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    public NodeSimilarityResult filteredNodeSimilarity(Graph graph, FilteredNodeSimilarityBaseConfig configuration) {
        var parameters = configuration.toFilteredParameters();
        var task = SimilarityAlgorithmTasks.filteredNodeSimilarity(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return progressTrackerManager.runAlgorithmAndManageProgressTracker(
            () -> similarityAlgorithms.filteredNodeSimilarity(graph, parameters, progressTracker),
            progressTracker,
            true
        );
    }
}
