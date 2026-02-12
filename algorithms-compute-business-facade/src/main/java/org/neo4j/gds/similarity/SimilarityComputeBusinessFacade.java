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
package org.neo4j.gds.similarity;

import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.AlgorithmGraphStoreRequirementsBuilder;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnParametersSansNodeCount;
import org.neo4j.gds.similarity.filteredknn.FilteredKnnResult;
import org.neo4j.gds.similarity.knn.KnnParametersSansNodeCount;
import org.neo4j.gds.similarity.knn.KnnResult;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityParameters;
import org.neo4j.gds.similarity.nodesim.NodeSimilarityResult;
import org.neo4j.gds.similarity.validation.KnnAlgorithmRequirements;
import org.neo4j.gds.similarity.validation.NodeFilterValidation;
import org.neo4j.gds.similarity.validation.NodeSimilarityRequirement;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class SimilarityComputeBusinessFacade {

    // Global dependencies
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final SimilarityComputeFacade computeFacade;

    // Request scope dependencies
    private final User user;
    private final DatabaseId databaseId;

    public SimilarityComputeBusinessFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        SimilarityComputeFacade computeFacade,
        User user,
        DatabaseId databaseId
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.computeFacade = computeFacade;
        this.user = user;
        this.databaseId = databaseId;
    }

    public <TR> CompletableFuture<TR> knn(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        KnnParametersSansNodeCount parametersSansNodeCount,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<KnnResult>, TR> resultTransformerBuilder
    ) {

        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(new KnnAlgorithmRequirements(parametersSansNodeCount.nodePropertiesNames())),
            Optional.empty(),
            user,
            databaseId
        );
        var parameters = parametersSansNodeCount.finalize(graphResources.graph().nodeCount());

        return computeFacade.knn(
            graphResources.graph(),
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> filteredKnn(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        FilteredKnnParametersSansNodeCount parametersSansNodeCount,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<FilteredKnnResult>, TR> resultTransformerBuilder
    ) {

        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new AlgorithmGraphStoreRequirementsBuilder()
                .withAlgorithmRequirement(
                    new KnnAlgorithmRequirements(parametersSansNodeCount.knnParametersSansNodeCount().nodePropertiesNames())
                )
                .withAlgorithmRequirement(
                    new NodeFilterValidation(
                        parametersSansNodeCount.filteringParameters().sourceFilter(),
                        parametersSansNodeCount.filteringParameters().targetFilter()
                    )
                ).build(),
            Optional.empty(),
            user,
            databaseId
        );
        var parameters = parametersSansNodeCount.finalize(graphResources.graph().nodeCount());

        return computeFacade.filteredKnn(
            graphResources.graph(),
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

    public <TR> CompletableFuture<TR> nodeSimilarity(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        NodeSimilarityParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<NodeSimilarityResult>, TR> resultTransformerBuilder
    ) {

        // Fetch the Graph the algorithm will operate on
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            graphName,
            graphParameters,
            relationshipProperty,
            new GraphStoreValidation(
                    new NodeSimilarityRequirement(
                        parameters.useComponents(),
                        parameters.componentProperty()
                    )
            ),
            Optional.empty(),
            user,
            databaseId
        );

        return computeFacade.nodeSimilarity(
            graphResources.graph(),
            parameters,
            jobId,
            logProgress
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

}
