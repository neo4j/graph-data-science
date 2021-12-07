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
package org.neo4j.gds;

import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.GraphDimensions;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.mem.MemoryTree;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.validation.ValidationConfiguration;
import org.neo4j.gds.validation.Validator;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public abstract class AlgoBaseProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig> extends BaseProc {

    protected static final String STATS_DESCRIPTION = "Executes the algorithm and returns result statistics without writing the result to Neo4j.";

    protected String procName() {
        return this.getClass().getSimpleName();
    }

    public ProcConfigParser<CONFIG> configParser() {
        return new AlgoConfigParser<>(username(), AlgoBaseProc.this::newConfig);
    }

    protected abstract CONFIG newConfig(
        String username,
        CypherMapWrapper config
    );

    protected abstract GraphAlgorithmFactory<ALGO, CONFIG> algorithmFactory();

    protected ComputationResult<ALGO, ALGO_RESULT, CONFIG> compute(
        String graphName,
        Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        return compute(graphName, configuration, true, true);
    }

    protected ComputationResult<ALGO, ALGO_RESULT, CONFIG> compute(
        String graphName,
        Map<String, Object> configuration,
        boolean releaseAlgorithm,
        boolean releaseTopology
    ) {
        return procedureExecutor().compute(graphName, configuration, releaseAlgorithm, releaseTopology);
    }

    /**
     * Returns a single node property that has been produced by the procedure.
     */
    protected NodeProperties nodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        throw new UnsupportedOperationException("Procedure must implement org.neo4j.gds.AlgoBaseProc.nodeProperty");
    }

    protected Stream<MemoryEstimateResult> computeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        CONFIG algoConfig = configParser().processInput(algoConfiguration);
        GraphDimensions graphDimensions;
        Optional<MemoryEstimation> memoryEstimation;

        if (graphNameOrConfiguration instanceof Map) {
            var memoryEstimationGraphConfigParser = new MemoryEstimationGraphConfigParser(username());
            var graphCreateConfig = memoryEstimationGraphConfigParser.processInput(graphNameOrConfiguration);
            var graphStoreCreator = graphCreateConfig.isFictitiousLoading()
                ? new FictitiousGraphStoreLoader(graphCreateConfig)
                : new GraphStoreFromDatabaseLoader(graphCreateConfig, username(), graphLoaderContext());

            graphDimensions = graphStoreCreator.graphDimensions();
            memoryEstimation = Optional.of(graphStoreCreator.memoryEstimation());
        } else if (graphNameOrConfiguration instanceof String) {
            graphDimensions = new GraphStoreFromCatalogLoader(
                (String) graphNameOrConfiguration,
                algoConfig,
                username(),
                databaseId(),
                isGdsAdmin()
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
            algorithmFactory(),
            algoConfig
        );

        return Stream.of(new MemoryEstimateResult(memoryTreeWithDimensions));
    }

    public ValidationConfiguration<CONFIG> getValidationConfig() {
        return ValidationConfiguration.empty();
    }

    protected Validator<CONFIG> validator() {
        return new Validator<>(getValidationConfig());
    }

    private ProcedureExecutor<ALGO, ALGO_RESULT, CONFIG> procedureExecutor() {
        return new ProcedureExecutor<>(
            configParser(),
            memoryUsageValidator(),
            validator(),
            algorithmFactory(),
            transaction,
            log,
            taskRegistryFactory,
            procName(),
            username(),
            databaseId(),
            isGdsAdmin(),
            allocationTracker()
        );
    }

    private MemoryTreeWithDimensions procedureMemoryEstimation(
        GraphDimensions dimensions,
        Optional<MemoryEstimation> maybeGraphMemoryEstimation,
        GraphAlgorithmFactory<ALGO, CONFIG> algorithmFactory,
        CONFIG config
    ) {
        MemoryEstimations.Builder estimationBuilder = MemoryEstimations.builder("Memory Estimation");

        maybeGraphMemoryEstimation.map(graphEstimation -> estimationBuilder.add("graph", graphEstimation));

        estimationBuilder.add("algorithm", algorithmFactory.memoryEstimation(config));

        MemoryTree memoryTree = estimationBuilder.build().estimate(dimensions, config.concurrency());
        return new MemoryTreeWithDimensions(memoryTree, dimensions);
    }

    @ValueClass
    public interface ComputationResult<A extends Algorithm<A, RESULT>, RESULT, CONFIG extends AlgoBaseConfig> {
        long createMillis();

        long computeMillis();

        @Nullable
        A algorithm();

        @Nullable
        RESULT result();

        Graph graph();

        GraphStore graphStore();

        CONFIG config();

        @Value.Default
        default boolean isGraphEmpty() {
            return false;
        }
    }
}
