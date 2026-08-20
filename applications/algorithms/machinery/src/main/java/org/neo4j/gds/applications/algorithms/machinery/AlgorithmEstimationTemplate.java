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
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.loading.validation.GraphStoreValidation;
import org.neo4j.gds.core.loading.validation.NoAlgorithmRequirements;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.memest.FictitiousGraphStoreEstimationService;
import org.neo4j.gds.memest.GraphMemoryEstimation;
import org.neo4j.gds.memest.MemoryEstimationGraphConfigParser;

import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * All estimations look the same modulo some small variation. So this calls for something like Template Method.
 * But we do not like inheritance, so the hooks are injected
 */
public class AlgorithmEstimationTemplate {
    // global scoped dependencies
    private final FictitiousGraphStoreEstimationService fictitiousGraphStoreEstimationService;
    private final GraphStoreCatalogService graphStoreCatalogService;

    // request scoped parameters and services
    private final DatabaseGraphStoreEstimationService databaseGraphStoreEstimationService;
    private final RequestScopedDependencies requestScopedDependencies;
    private final GraphDimensionsFactory graphDimensionsFactory = new GraphDimensionsFactory();

    public AlgorithmEstimationTemplate(
        FictitiousGraphStoreEstimationService fictitiousGraphStoreEstimationService,
        GraphStoreCatalogService graphStoreCatalogService,
        DatabaseGraphStoreEstimationService databaseGraphStoreEstimationService,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.fictitiousGraphStoreEstimationService = fictitiousGraphStoreEstimationService;
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.databaseGraphStoreEstimationService = databaseGraphStoreEstimationService;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    public <CONFIGURATION extends AlgoBaseConfig> MemoryEstimateResult estimate(
        CONFIGURATION configuration,
        Object graphNameOrConfiguration,
        MemoryEstimation memoryEstimation
    ) {
        return estimate(
            configuration.toGraphParameters(),
            graphNameOrConfiguration,
            memoryEstimation,
            configuration.concurrency(), DimensionTransformer.DISABLED
        );
    }

    public MemoryEstimateResult estimate(
        GraphParameters graphParameters,
        Object graphNameOrConfiguration,
        MemoryEstimation memoryEstimation,
        Concurrency concurrency
    ) {
        return estimate(
            graphParameters,
            graphNameOrConfiguration,
            memoryEstimation,
            concurrency, DimensionTransformer.DISABLED
        );
    }

    public MemoryEstimateResult estimate(
        GraphParameters graphParameters,
        Object graphNameOrConfiguration,
        MemoryEstimation memoryEstimation,
        Concurrency concurrency,
        DimensionTransformer dimensionTransformer
    ) {
        var estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        if (graphNameOrConfiguration instanceof Map graphConfig) {
            var memoryEstimationGraphConfigParser = new MemoryEstimationGraphConfigParser(requestScopedDependencies.user().getUsername());
            var projectionConfiguration = memoryEstimationGraphConfigParser.parse(graphConfig);

            var graphMemoryEstimation = estimate(projectionConfiguration);

            estimationBuilder.add("graph", graphMemoryEstimation.estimateMemoryUsageAfterLoading());

            var graphDimensions = graphMemoryEstimation.dimensions();

            var transformedDimensions = dimensionTransformer.transform(graphDimensions);

            return estimate(
                estimationBuilder,
                memoryEstimation,
                transformedDimensions,
                concurrency
            );
        }

        if (graphNameOrConfiguration instanceof String rawGraphName) {
            GraphName graphName = GraphName.parse(rawGraphName);

            var graphDimensions = dimensionsFromActualGraph(graphName, graphParameters);

            var transformedDimensions = dimensionTransformer.transform(graphDimensions);

            return estimate(estimationBuilder, memoryEstimation, transformedDimensions, concurrency);
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected `graphNameOrConfiguration` to be of type String or Map, but got `%s`",
            graphNameOrConfiguration.getClass().getSimpleName()
        ));
    }

    private GraphMemoryEstimation estimate(GraphProjectConfig projectionConfiguration) {
        return projectionConfiguration.isFictitiousLoading()
            ? fictitiousGraphStoreEstimationService.estimate(
            requestScopedDependencies.correlationId(),
            projectionConfiguration
        )
            : databaseGraphStoreEstimationService.estimate(
                requestScopedDependencies.correlationId(),
                projectionConfiguration
            );
    }

    private GraphDimensions dimensionsFromActualGraph(
        GraphName graphName,
        GraphParameters graphParameters
    ) {
        var graphResources = graphStoreCatalogService.fetchGraphResources(
            requestScopedDependencies.databaseId(),
            graphName,
            requestScopedDependencies.user(),
            graphParameters,
            Optional.empty(),
            new GraphStoreValidation(new NoAlgorithmRequirements()),
            true,
            Optional.empty()
        );

        return graphDimensionsFactory.graphDimensions(graphResources, graphParameters);
    }

    private MemoryEstimateResult estimate(
        MemoryEstimations.Builder estimationBuilder,
        MemoryEstimation memoryEstimation,
        GraphDimensions graphDimensions,
        Concurrency concurrency
    ) {
        var rootEstimation = estimationBuilder
            .add("algorithm", memoryEstimation)
            .build();

        var memoryTree = rootEstimation.estimate(graphDimensions, concurrency);

        return MemoryEstimateResultFactory.from(memoryTree, graphDimensions);
    }
}
