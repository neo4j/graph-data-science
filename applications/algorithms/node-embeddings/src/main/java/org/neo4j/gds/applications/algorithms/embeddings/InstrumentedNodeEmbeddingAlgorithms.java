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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.embeddings.fastrp.FastRPBaseConfig;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.hashgnn.HashGNNConfig;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public final class InstrumentedNodeEmbeddingAlgorithms {
    private final NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation;
    private final TrackedNodeEmbeddingAlgorithms trackedAlgorithms;
    private final LaunchConvenience launchConvenience;

    private InstrumentedNodeEmbeddingAlgorithms(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation,
        TrackedNodeEmbeddingAlgorithms trackedAlgorithms,
        LaunchConvenience launchConvenience
    ) {
        this.estimation = estimation;
        this.trackedAlgorithms = trackedAlgorithms;
        this.launchConvenience = launchConvenience;
    }

    public static InstrumentedNodeEmbeddingAlgorithms create(
        NodeEmbeddingAlgorithmsEstimationModeBusinessFacade estimation,
        NodeEmbeddingAlgorithms algorithms,
        ProgressTrackerCreator progressTrackerCreator,
        LaunchConvenience launchConvenience
    ) {
        var trackedAlgorithms = new TrackedNodeEmbeddingAlgorithms(algorithms, progressTrackerCreator);

        return new InstrumentedNodeEmbeddingAlgorithms(estimation, trackedAlgorithms, launchConvenience);
    }

    <RESULT, METADATA> CompletableFuture<RESULT> fastRP(
        GraphName graphName,
        FastRPBaseConfig configuration,
        Optional<SideEffect<FastRPResult, METADATA>> sideEffect,
        ResultRenderer<FastRPResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreFilteredETL(new FeaturePropertiesMustExistOnAllNodeLabels(configuration.featureProperties()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> trackedAlgorithms.fastRP(graph, configuration),
            () -> estimation.fastRP(configuration),
            AlgorithmLabel.FastRP,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> hashGnn(
        GraphName graphName,
        HashGNNConfig configuration,
        Optional<SideEffect<HashGNNResult, METADATA>> sideEffect,
        ResultRenderer<HashGNNResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .withGraphStoreFilteredETL(new FeaturePropertiesMustExistOnAllNodeLabels(configuration.featureProperties()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> trackedAlgorithms.hashGnn(graph, configuration),
            () -> estimation.hashGnn(configuration),
            AlgorithmLabel.HashGNN,
            sideEffect,
            resultRenderer
        );
    }
}
