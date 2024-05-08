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

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.mem.MemoryEstimations;
import org.neo4j.gds.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.memest.DatabaseGraphStoreEstimationService;
import org.neo4j.gds.memest.FictitiousGraphStoreEstimationService;
import org.neo4j.gds.memest.GraphMemoryEstimation;
import org.neo4j.gds.memest.MemoryEstimationGraphConfigParser;

import java.util.Map;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

/**
 * All estimations look the same modulo some small variation. So this calls for something like Template Method.
 * But we do not like inheritance, so the hooks are injected
 */
public class AlgorithmEstimationTemplate {
    private final FictitiousGraphStoreEstimationService fictitiousGraphStoreEstimationService = new FictitiousGraphStoreEstimationService();

    // global scoped dependencies
    private final GraphStoreCatalogService graphStoreCatalogService;

    // request scoped parameters and services
    private final DatabaseGraphStoreEstimationService databaseGraphStoreEstimationService;
    private final RequestScopedDependencies requestScopedDependencies;

    public AlgorithmEstimationTemplate(
        GraphStoreCatalogService graphStoreCatalogService,
        DatabaseGraphStoreEstimationService databaseGraphStoreEstimationService,
        RequestScopedDependencies requestScopedDependencies
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.databaseGraphStoreEstimationService = databaseGraphStoreEstimationService;
        this.requestScopedDependencies = requestScopedDependencies;
    }

    public <CONFIGURATION extends AlgoBaseConfig> MemoryEstimateResult estimate(
        CONFIGURATION configuration,
        Object graphNameOrConfiguration,
        MemoryEstimation memoryEstimation
    ) {
        var estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        if (graphNameOrConfiguration instanceof Map) {
            var memoryEstimationGraphConfigParser = new MemoryEstimationGraphConfigParser(requestScopedDependencies.getUser().getUsername());
            var projectionConfiguration = memoryEstimationGraphConfigParser.parse(graphNameOrConfiguration);

            var graphMemoryEstimation = estimate(projectionConfiguration);

            estimationBuilder.add("graph", graphMemoryEstimation.estimateMemoryUsageAfterLoading());

            return estimate(
                estimationBuilder,
                memoryEstimation,
                graphMemoryEstimation.dimensions(),
                configuration.concurrency()
            );
        }

        if (graphNameOrConfiguration instanceof String) {
            GraphName graphName = GraphName.parse((String) graphNameOrConfiguration);

            var graphDimensions = dimensionsFromActualGraph(graphName, configuration);

            return estimate(estimationBuilder, memoryEstimation, graphDimensions, configuration.concurrency());
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected `graphNameOrConfiguration` to be of type String or Map, but got `%s`",
            graphNameOrConfiguration.getClass().getSimpleName()
        ));
    }

    private GraphMemoryEstimation estimate(GraphProjectConfig projectionConfiguration) {
        return projectionConfiguration.isFictitiousLoading()
            ? fictitiousGraphStoreEstimationService.estimate(projectionConfiguration)
            : databaseGraphStoreEstimationService.estimate(projectionConfiguration);
    }

    private <CONFIGURATION extends AlgoBaseConfig> GraphDimensions dimensionsFromActualGraph(
        GraphName graphName,
        CONFIGURATION configuration
    ) {
        var graphStore = graphStoreCatalogService.getGraphStoreCatalogEntry(
            graphName,
            configuration,
            requestScopedDependencies.getUser(),
            requestScopedDependencies.getDatabaseId()
        ).graphStore();

        return GraphDimensionsComputer.of(graphStore, configuration);
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

        return new MemoryEstimateResult(new MemoryTreeWithDimensions(memoryTree, graphDimensions));
    }
}
