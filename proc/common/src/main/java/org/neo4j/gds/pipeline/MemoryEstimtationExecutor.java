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
package org.neo4j.gds.pipeline;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.FictitiousGraphStoreLoader;
import org.neo4j.gds.GraphStoreFromCatalogLoader;
import org.neo4j.gds.GraphStoreFromDatabaseLoader;
import org.neo4j.gds.MemoryEstimationGraphConfigParser;
import org.neo4j.gds.ProcConfigParser;
import org.neo4j.gds.api.GraphLoaderContext;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.kernel.database.NamedDatabaseId;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class MemoryEstimtationExecutor<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig
> {

    private final ProcConfigParser<CONFIG> configParser;
    private final AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory;
    private final Supplier<GraphLoaderContext> graphLoaderContextSupplier;
    private final String username;
    private final Supplier<NamedDatabaseId> databaseIdSupplier;
    private final boolean isGdsAdmin;

    public MemoryEstimtationExecutor(
        ProcConfigParser<CONFIG> configParser,
        AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory,
        Supplier<GraphLoaderContext> graphLoaderContextSupplier,
        Supplier<NamedDatabaseId> databaseIdSupplier,
        String username,
        boolean isGdsAdmin
    ) {
        this.configParser = configParser;
        this.algorithmFactory = algorithmFactory;
        this.graphLoaderContextSupplier = graphLoaderContextSupplier;
        this.username = username;
        this.databaseIdSupplier = databaseIdSupplier;
        this.isGdsAdmin = isGdsAdmin;
    }

    public Stream<MemoryEstimateResult> computeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        CONFIG algoConfig = configParser.processInput(algoConfiguration);
        GraphDimensions graphDimensions;
        Optional<MemoryEstimation> memoryEstimation;

        if (graphNameOrConfiguration instanceof Map) {
            var memoryEstimationGraphConfigParser = new MemoryEstimationGraphConfigParser(username);
            var graphCreateConfig = memoryEstimationGraphConfigParser.processInput(graphNameOrConfiguration);
            var graphStoreCreator = graphCreateConfig.isFictitiousLoading()
                ? new FictitiousGraphStoreLoader(graphCreateConfig)
                : new GraphStoreFromDatabaseLoader(graphCreateConfig, username, graphLoaderContextSupplier.get());

            graphDimensions = graphStoreCreator.graphDimensions();
            memoryEstimation = Optional.of(graphStoreCreator.memoryEstimation());
        } else if (graphNameOrConfiguration instanceof String) {
            graphDimensions = new GraphStoreFromCatalogLoader(
                (String) graphNameOrConfiguration,
                algoConfig,
                username,
                databaseIdSupplier.get(),
                isGdsAdmin
            ).graphDimensions();

            memoryEstimation = Optional.empty();
        } else {
            throw new IllegalArgumentException(formatWithLocale(
                "Expected `graphNameOrConfiguration` to be of type String or Map, but got",
                graphNameOrConfiguration.getClass().getSimpleName()
            ));
        }

        MemoryTreeWithDimensions memoryTreeWithDimensions = procedureMemoryEstimation(
            graphDimensions,
            memoryEstimation,
            algorithmFactory,
            algoConfig
        );

        return Stream.of(new MemoryEstimateResult(memoryTreeWithDimensions));
    }

    private MemoryTreeWithDimensions procedureMemoryEstimation(
        GraphDimensions dimensions,
        Optional<MemoryEstimation> maybeGraphMemoryEstimation,
        AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory,
        CONFIG config
    ) {
        MemoryEstimations.Builder estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        maybeGraphMemoryEstimation.map(graphEstimation -> estimationBuilder.add("graph", graphEstimation));

        estimationBuilder.add("algorithm", algorithmFactory.memoryEstimation(config));

        MemoryTree memoryTree = estimationBuilder.build().estimate(dimensions, config.concurrency());
        return new MemoryTreeWithDimensions(memoryTree, dimensions);
    }
}
