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
package org.neo4j.gds.algorithms.community;

import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.User;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.memest.FictitiousGraphStoreService;
import org.neo4j.gds.memest.GraphStoreFromDatabaseService;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.memest.MemoryEstimationGraphConfigParser;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.wcc.WccAlgorithmFactory;
import org.neo4j.gds.wcc.WccStreamConfig;

import java.util.Map;
import java.util.Optional;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class CommunityAlgorithmsEstimateBusinessFacade {

    private final GraphStoreCatalogService graphStoreCatalogService;

    private final FictitiousGraphStoreService fictitiousGraphStoreLoaderService;
    private final GraphStoreFromDatabaseService graphStoreFromDatabaseService;
    private final DatabaseId databaseId;
    private final User user;

    public CommunityAlgorithmsEstimateBusinessFacade(
        GraphStoreCatalogService graphStoreCatalogService,
        FictitiousGraphStoreService fictitiousGraphStoreLoaderService,
        GraphStoreFromDatabaseService graphStoreFromDatabaseService,
        DatabaseId databaseId,
        User user
    ) {
        this.graphStoreCatalogService = graphStoreCatalogService;
        this.fictitiousGraphStoreLoaderService = fictitiousGraphStoreLoaderService;
        this.graphStoreFromDatabaseService = graphStoreFromDatabaseService;
        this.databaseId = databaseId;
        this.user = user;
    }

    public MemoryEstimateResult estimateWcc(Object graphNameOrConfiguration, Map<String, Object> configuration) {
        var config = WccStreamConfig.of(CypherMapWrapper.create(configuration));

        GraphDimensions dimensions = null;
        Optional<MemoryEstimation> maybeGraphEstimation = Optional.empty();

        if (graphNameOrConfiguration instanceof Map) {
            var memoryEstimationGraphConfigParser = new MemoryEstimationGraphConfigParser(user.getUsername());
            var graphProjectConfig = memoryEstimationGraphConfigParser.parse(graphNameOrConfiguration);

            var graphMemoryEstimation = graphProjectConfig.isFictitiousLoading()
                ? fictitiousGraphStoreLoaderService.estimate(graphProjectConfig)
                : graphStoreFromDatabaseService.estimate(graphProjectConfig);

            dimensions = graphMemoryEstimation.dimensions();
            maybeGraphEstimation = Optional.of(graphMemoryEstimation.estimateMemoryUsageAfterLoading());
        } else if (graphNameOrConfiguration instanceof String) {
            var graphStore = graphStoreCatalogService.getGraphWithGraphStore(
                GraphName.parse(
                    (String) graphNameOrConfiguration),
                config,
                config.relationshipWeightProperty(),
                user,
                databaseId
            ).getRight();
            dimensions = GraphDimensionsComputer.of(graphStore, config);
            // This here is empty because the graph is already in the catalog,
            // and we don't have to account for the memory it would occupy.
            maybeGraphEstimation = Optional.empty();
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Expected `graphNameOrConfiguration` to be of type String or Map, but got `%s`",
                graphNameOrConfiguration.getClass().getSimpleName()
            ));
        }

        var algorithmFactory = new WccAlgorithmFactory<>();

        MemoryEstimations.Builder estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        GraphDimensions extendedDimension = algorithmFactory.estimatedGraphDimensionTransformer(dimensions, config);

        maybeGraphEstimation.ifPresent(graphMemoryEstimation -> estimationBuilder.add("graph", graphMemoryEstimation));
        estimationBuilder.add("algorithm", algorithmFactory.memoryEstimation(config));

        MemoryTree memoryTree = estimationBuilder.build().estimate(extendedDimension, config.concurrency());

        MemoryTreeWithDimensions memoryTreeWithDimensions = new MemoryTreeWithDimensions(memoryTree, dimensions);

        return new MemoryEstimateResult(memoryTreeWithDimensions);
    }



}
