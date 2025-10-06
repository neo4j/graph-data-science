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
package org.neo4j.gds.embeddings;

import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.NoAlgorithmValidation;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.embeddings.fastrp.FastRPParameters;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.embeddings.hashgnn.HashGNNParameters;
import org.neo4j.gds.embeddings.hashgnn.HashGNNResult;
import org.neo4j.gds.embeddings.node2vec.Node2VecParameters;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.embeddings.validation.FeaturePropertiesMustExistOnAllNodeLabels;
import org.neo4j.gds.embeddings.validation.Node2VecGraphValidation;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class NodeEmbeddingComputeBusinessFacade {

    // Global dependencies
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final NodeEmbeddingComputeFacade computeFacade;

    // Request scope dependencies
    private final User user;
    private final DatabaseId databaseId;

    public NodeEmbeddingComputeBusinessFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        NodeEmbeddingComputeFacade computeFacade,
        User user,
        DatabaseId databaseId
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.computeFacade = computeFacade;
        this.user = user;
        this.databaseId = databaseId;
    }

    public <TR> CompletableFuture<TR> fastRP(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        FastRPParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<FastRPResult>, TR> resultTransformerBuilder
    ) {

        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new FeaturePropertiesMustExistOnAllNodeLabels(parameters.featureProperties()),
            Optional.empty(),
            user,
            databaseId
        );

        return computeFacade.fastRP(
            graphResources.graph(),
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> hashGnn(
        GraphName graphName,
        GraphParameters graphParameters,
        // FIXME: `relationshipTypes` is only used to create progress tracker tasks, and in there only the count...
        List<String> relationshipTypes,
        HashGNNParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<HashGNNResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            Optional.empty(),
            new FeaturePropertiesMustExistOnAllNodeLabels(parameters.featureProperties()),
            Optional.empty(),
            user,
            databaseId
        );

        return computeFacade.hashGnn(
            graphResources.graph(),
            parameters,
            relationshipTypes,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> node2Vec(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        Node2VecParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<Node2VecResult>, TR> resultTransformerBuilder
    ) {
        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new NoAlgorithmValidation(),
            Optional.of(new Node2VecGraphValidation(
                parameters.samplingWalkParameters().walksPerNode(),
                parameters.samplingWalkParameters().walkLength()
            )),
            user,
            databaseId
        );

        return computeFacade.node2Vec(
            graphResources.graph(),
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }
}
