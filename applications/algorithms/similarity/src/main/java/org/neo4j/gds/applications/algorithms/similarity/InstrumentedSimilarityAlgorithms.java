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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnBaseConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.knn.KnnBaseConfig;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public final class InstrumentedSimilarityAlgorithms {
    private final TrackedSimilarityAlgorithms algorithms;
    private final SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;

    private InstrumentedSimilarityAlgorithms(
        TrackedSimilarityAlgorithms algorithms,
        SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        this.algorithms = algorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
    }

    public static InstrumentedSimilarityAlgorithms create(
        SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience,
        ProgressTrackerCreator progressTrackerCreator,
        TerminationFlag terminationFlag
    ) {
        var similarityAlgorithms = new SimilarityAlgorithms(terminationFlag);
        var trackedAlgorithms = new TrackedSimilarityAlgorithms(similarityAlgorithms, progressTrackerCreator);

        return new InstrumentedSimilarityAlgorithms(trackedAlgorithms, estimationFacade, launchConvenience);
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> filteredKnn(
        GraphName graphName,
        FilteredKnnBaseConfig configuration,
        Optional<SideEffect<FilteredKnnResult, METADATA>> sideEffect,
        ResultRenderer<FilteredKnnResult, RESULT, METADATA> resultRenderer
    ) {
        var filteredKnnParameters = configuration.toFilteredKnnParameters();

        var graphLoadETLBuilder = GraphLoadETLBuilder.from(configuration)
            .withGraphStoreFilteredETL(new KnnETL(filteredKnnParameters.knnParametersSansNodeCount().nodePropertiesNames()))
            .withGraphStoreFilteredETL(new NodeFilterETL(
                filteredKnnParameters.filteringParameters().sourceFilter(),
                filteredKnnParameters.filteringParameters().targetFilter()
            ));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.filteredKnn(graph, configuration),
            () -> estimationFacade.filteredKnn(configuration),
            AlgorithmLabel.FilteredKNN,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> filteredNodeSimilarity(
        GraphName graphName,
        FilteredNodeSimilarityBaseConfig configuration,
        Optional<SideEffect<NodeSimilarityResult, METADATA>> sideEffect,
        ResultRenderer<NodeSimilarityResult, RESULT, METADATA> resultRenderer
    ) {
        var filteredParameters = configuration.toFilteredParameters();

        var graphLoadETLBuilder = GraphLoadETLBuilder.from(configuration)
            .withGraphStoreFilteredETL(new NodeFilterETL(
                filteredParameters.filteringParameters().sourceFilter(),
                filteredParameters.filteringParameters().targetFilter()
            ))
            .withGraphStoreFilteredETL(new NodeSimilarityETL(
                filteredParameters.nodeSimilarityParameters().useComponents(),
                filteredParameters.nodeSimilarityParameters().componentProperty()
            ));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.filteredNodeSimilarity(graph, configuration),
            () -> estimationFacade.filteredNodeSimilarity(configuration),
            AlgorithmLabel.FilteredNodeSimilarity,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> knn(
        GraphName graphName,
        KnnBaseConfig configuration,
        Optional<SideEffect<KnnResult, METADATA>> sideEffect,
        ResultRenderer<KnnResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = GraphLoadETLBuilder.from(configuration)
            .withGraphStoreFilteredETL(new KnnETL(configuration.toParameters().nodePropertiesNames()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.knn(graph, configuration),
            () -> estimationFacade.knn(configuration),
            AlgorithmLabel.KNN,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> nodeSimilarity(
        GraphName graphName,
        NodeSimilarityBaseConfig configuration,
        Optional<SideEffect<NodeSimilarityResult, METADATA>> sideEffect,
        ResultRenderer<NodeSimilarityResult, RESULT, METADATA> resultRenderer
    ) {
        var parameters = configuration.toParameters();

        var graphLoadETLBuilder = GraphLoadETLBuilder.from(configuration)
            .withGraphStoreFilteredETL(new NodeSimilarityETL(parameters.useComponents(), parameters.componentProperty()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.nodeSimilarity(graph, configuration),
            () -> estimationFacade.nodeSimilarity(configuration),
            AlgorithmLabel.NodeSimilarity,
            sideEffect,
            resultRenderer
        );
    }
}
