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
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnBaseConfig;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.filterednodesim.FilteredNodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityBaseConfig;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class InstrumentedSimilarityAlgorithms {
    private final TrackedSimilarityAlgorithms algorithms;
    private final SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;

    public InstrumentedSimilarityAlgorithms(
        TrackedSimilarityAlgorithms algorithms,
        SimilarityAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        this.algorithms = algorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> filteredKnn(
        GraphName graphName,
        FilteredKnnBaseConfig configuration,
        Optional<SideEffect<FilteredKnnResult, METADATA>> sideEffect,
        ResultRenderer<FilteredKnnResult, RESULT, METADATA> resultRenderer
    ) {
        var filteredKnnParameters = configuration.toFilteredKnnParameters();

        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new KnnETL(filteredKnnParameters.knnParametersSansNodeCount().nodePropertiesNames()))
            .with(new NodeFilterETL(
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

        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new NodeFilterETL(
                filteredParameters.filteringParameters().sourceFilter(),
                filteredParameters.filteringParameters().targetFilter()
            ))
            .with(new NodeSimilarityETL(
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

    public <RESULT, METADATA> CompletableFuture<RESULT> nodeSimilarity(
        GraphName graphName,
        NodeSimilarityBaseConfig configuration,
        Optional<SideEffect<NodeSimilarityResult, METADATA>> sideEffect,
        ResultRenderer<NodeSimilarityResult, RESULT, METADATA> resultRenderer
    ) {
        var parameters = configuration.toParameters();

        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new NodeSimilarityETL(parameters.useComponents(), parameters.componentProperty()));

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
