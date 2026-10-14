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
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.embeddings.node2vec.Node2VecParameters;
import org.neo4j.gds.embeddings.node2vec.Node2VecResult;
import org.neo4j.gds.embeddings.validation.Node2VecGraphValidation;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformerBuilder;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class NodeEmbeddingComputeBusinessFacade {

    // Global dependencies
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final NodeEmbeddingComputeFacade computeFacade;

    public NodeEmbeddingComputeBusinessFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        NodeEmbeddingComputeFacade computeFacade
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.computeFacade = computeFacade;
    }

    public <TR> CompletableFuture<TR> node2Vec(
        GraphName graphName,
        GraphParameters graphParameters,
        Optional<String> relationshipProperty,
        Node2VecParameters parameters,
        JobId jobId,
        boolean logProgress,
        ResultTransformerBuilder<TimedAlgorithmResult<Node2VecResult>, TR> resultTransformerBuilder,
        User user,
        DatabaseId databaseId,
        TerminationFlag terminationFlag
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            relationshipProperty,
            GraphStoreValidation.DISABLED,
            true,
            Optional.of(new Node2VecGraphValidation(
                parameters.samplingWalkParameters().walksPerNode(),
                parameters.samplingWalkParameters().walkLength()
            ))
        );

        return computeFacade.node2Vec(
            graphResources.graph(),
            parameters,
            jobId,
            logProgress,
            terminationFlag
        ).thenApply(resultTransformerBuilder.build(graphResources));
    }

}
