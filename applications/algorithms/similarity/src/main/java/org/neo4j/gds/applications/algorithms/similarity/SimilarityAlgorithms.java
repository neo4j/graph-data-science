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

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmMachinery;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.tasks.Task;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.similarity.filteredknn.FilteredKnn;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnBaseConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.filtering.NodeFilter;
import org.neo4j.gds.similarity.knn.ImmutableKnnContext;
import org.neo4j.gds.similarity.knn.Knn;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;
import org.neo4j.gds.similarity.knn.KnnContext;
import org.neo4j.gds.similarity.knn.KnnNeighborFilterFactory;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.knn.metrics.SimilarityComputer;
import org.neo4j.gds.similarity.nodesim.NodeSimilarity;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.wcc.WccAlgorithmFactory;

import java.util.List;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.FilteredKNN;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.FilteredNodeSimilarity;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.KNN;

public class SimilarityAlgorithms {
    private final AlgorithmMachinery algorithmMachinery = new AlgorithmMachinery();

    private final ProgressTrackerCreator progressTrackerCreator;
    private final RequestScopedDependencies requestScopedDependencies;

    public SimilarityAlgorithms(
        ProgressTrackerCreator progressTrackerCreator,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.progressTrackerCreator = progressTrackerCreator;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    FilteredKnnResult filteredKnn(Graph graph, FilteredKnnBaseConfig configuration) {
        long nodeCount = graph.nodeCount();

        Task task = Tasks.task(
            FilteredKNN.value,
            Tasks.leaf("Initialize random neighbors", nodeCount),
            Tasks.iterativeDynamic(
                "Iteration",
                () -> List.of(
                    Tasks.leaf("Split old and new neighbors", nodeCount),
                    Tasks.leaf("Reverse old and new neighbors", nodeCount),
                    Tasks.leaf("Join neighbors", nodeCount)
                ),
                configuration.maxIterations()
            )
        );
        var progressTracker = progressTrackerCreator.createProgressTracker(
            configuration,
            task
        );
        var knnContext = ImmutableKnnContext
            .builder()
            .progressTracker(progressTracker)
            .executor(DefaultPool.INSTANCE)
            .build();

        var algorithm = selectAlgorithmConfiguration(graph, configuration, knnContext);

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    NodeSimilarityResult filteredNodeSimilarity(Graph graph, FilteredNodeSimilarityBaseConfig configuration) {
        var sourceNodeFilter = configuration.sourceNodeFilter().toNodeFilter(graph);
        var targetNodeFilter = configuration.targetNodeFilter().toNodeFilter(graph);

        var task = Tasks.task(
            FilteredNodeSimilarity.value,
            filteredNodeSimilarityProgressTask(graph, configuration.useComponents().computeComponents()),
            Tasks.leaf("compare node pairs")
        );

        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new NodeSimilarity(
            graph,
            configuration.toParameters(),
            configuration.concurrency(),
            DefaultPool.INSTANCE,
            progressTracker,
            sourceNodeFilter,
            targetNodeFilter,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    KnnResult knn(Graph graph, KnnBaseConfig configuration) {
        var parameters = configuration.toParameters().finalize(graph.nodeCount());

        long nodeCount = graph.nodeCount();

        Task task = Tasks.task(
            KNN.value,
            Tasks.leaf("Initialize random neighbors", nodeCount),
            Tasks.iterativeDynamic(
                "Iteration",
                () -> List.of(
                    Tasks.leaf("Split old and new neighbors", nodeCount),
                    Tasks.leaf("Reverse old and new neighbors", nodeCount),
                    Tasks.leaf("Join neighbors", nodeCount)
                ),
                configuration.maxIterations()
            )
        );


        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = Knn.create(
            graph,
            parameters,
            SimilarityComputer.ofProperties(graph, parameters.nodePropertySpecs()),
            new KnnNeighborFilterFactory(graph.nodeCount()),
            ImmutableKnnContext
                .builder()
                .progressTracker(progressTracker)
                .executor(DefaultPool.INSTANCE)
                .build(),
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    NodeSimilarityResult nodeSimilarity(Graph graph, NodeSimilarityBaseConfig configuration) {
        Task task = Tasks.task(
            LabelForProgressTracking.NodeSimilarity.value,
            configuration.useComponents().computeComponents()
                ? Tasks.task(
                "prepare",
                new WccAlgorithmFactory<>().progressTask(graph),
                Tasks.leaf("initialize", graph.relationshipCount())
            )
                : Tasks.leaf("prepare", graph.relationshipCount()),
            Tasks.leaf("compare node pairs")
        );

        var progressTracker = progressTrackerCreator.createProgressTracker(configuration, task);

        var algorithm = new NodeSimilarity(
            graph,
            configuration.toParameters(),
            configuration.concurrency(),
            DefaultPool.INSTANCE,
            progressTracker,
            NodeFilter.ALLOW_EVERYTHING,
            NodeFilter.ALLOW_EVERYTHING,
            requestScopedDependencies.getTerminationFlag()
        );

        return algorithmMachinery.runAlgorithmsAndManageProgressTracker(algorithm, progressTracker, true);
    }

    private Task filteredNodeSimilarityProgressTask(Graph graph, boolean runWcc) {
        if (runWcc) {
            return Tasks.task(
                "prepare",
                new WccAlgorithmFactory<>().progressTask(graph),
                Tasks.leaf("initialize", graph.relationshipCount())
            );
        }
        return Tasks.leaf("prepare", graph.relationshipCount());
    }

    private FilteredKnn selectAlgorithmConfiguration(
        Graph graph,
        FilteredKnnBaseConfig configuration,
        KnnContext knnContext
    ) {
        if (configuration.seedTargetNodes()) {
            return FilteredKnn.createWithDefaultSeeding(
                graph,
                configuration,
                knnContext,
                requestScopedDependencies.getTerminationFlag()
            );
        }

        return FilteredKnn.createWithoutSeeding(
            graph,
            configuration,
            knnContext,
            requestScopedDependencies.getTerminationFlag()
        );
    }
}
