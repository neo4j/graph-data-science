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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnBaseConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

public class SimilarityAlgorithmsBusinessFacade {

    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();
    private final SimilarityAlgorithms similarityAlgorithms;
    private final ProgressTrackerCreator progressTrackerCreator;
    private final SimilarityAlgorithmTasks tasks = new SimilarityAlgorithmTasks();

    public SimilarityAlgorithmsBusinessFacade(
        SimilarityAlgorithms similarityAlgorithms,
        ProgressTrackerCreator progressTrackerCreator
    ) {
        this.similarityAlgorithms = similarityAlgorithms;
        this.progressTrackerCreator = progressTrackerCreator;
    }

    FilteredKnnResult filteredKnn(Graph graph, FilteredKnnBaseConfig configuration) {
        var parameters = configuration.toFilteredKnnParameters().finalize(graph.nodeCount());
        var task = tasks.filteredKnn(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> similarityAlgorithms.filteredKnn(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );
    }

    public NodeSimilarityResult filteredNodeSimilarity(Graph graph, FilteredNodeSimilarityBaseConfig configuration) {
        var parameters = configuration.toFilteredParameters();
        var task = tasks.filteredNodeSimilarity(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> similarityAlgorithms.filteredNodeSimilarity(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );
    }


    KnnResult knn(Graph graph, KnnBaseConfig configuration) {
        var parameters = configuration.toParameters().finalize(graph.nodeCount());
        var task = tasks.knn(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> similarityAlgorithms.knn(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );

    }

    public NodeSimilarityResult nodeSimilarity(Graph graph, NodeSimilarityBaseConfig configuration) {
        var parameters = configuration.toParameters();
        var task = tasks.nodeSimilarity(graph, parameters);
        var progressTracker = progressTrackerCreator.createProgressTracker(task, configuration);

        return algorithmMachinery.getResult(
            () -> similarityAlgorithms.nodeSimilarity(graph, parameters, progressTracker),
            progressTracker,
            parameters.concurrency()
        );
    }


}
