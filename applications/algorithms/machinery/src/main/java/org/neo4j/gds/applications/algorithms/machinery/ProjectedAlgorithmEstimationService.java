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
package org.neo4j.gds.applications.algorithms.machinery;
import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;

import java.util.Optional;

public class ProjectedAlgorithmEstimationService {
    private final GraphStoreCatalogService graphStoreCatalogService;
    private final GraphDimensionsFactory graphDimensionsFactory = new GraphDimensionsFactory();

    public ProjectedAlgorithmEstimationService(
        GraphStoreCatalogService graphStoreCatalogService
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
    }

    public MemoryEstimateResult estimate(
        GraphName graphName,
        GraphParameters graphParameters,
        MemoryEstimation memoryEstimation,
        Concurrency concurrency,
        DimensionTransformer dimensionTransformer,
        DatabaseId databaseId,
        User user
    ) {
        var estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        var graphDimensions = dimensionsFromActualGraph(graphName, graphParameters,databaseId,user);

        var transformedDimensions = dimensionTransformer.transform(graphDimensions);

        return AlgorithmEstimator.estimate(estimationBuilder, memoryEstimation, transformedDimensions, concurrency);
    }

    private GraphDimensions dimensionsFromActualGraph(
        GraphName graphName,
        GraphParameters graphParameters,
        DatabaseId databaseId,
        User user
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            databaseId,
            graphName,
            user,
            graphParameters,
            Optional.empty(),
            GraphStoreValidation.DISABLED,
            true,
            Optional.empty()
        );

        return graphDimensionsFactory.graphDimensions(graphResources, graphParameters);
    }
}
