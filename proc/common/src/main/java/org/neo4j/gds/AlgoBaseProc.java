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

import org.eclipse.collections.api.tuple.Pair;
import org.immutables.value.Value;
import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.annotation.ValueClass;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.MemoryTreeWithDimensions;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.validation.ValidationConfiguration;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public abstract class AlgoBaseProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig> extends BaseProc {

    protected static final String STATS_DESCRIPTION = "Executes the algorithm and returns result statistics without writing the result to Neo4j.";
    protected String procName() {
        return this.getClass().getSimpleName();
    }

    public ProcConfigParser<CONFIG> configParser() {
        return new DefaultProcConfigParser<>(username(), AlgoBaseProc.this::newConfig);
    }

    public ProcedureMemoryEstimation<ALGO, ALGO_RESULT, CONFIG> procedureMemoryEstimation(GraphStoreLoader graphStoreLoader) {
        return new ProcedureMemoryEstimation<>(graphStoreLoader, algorithmFactory());
    }

    protected abstract CONFIG newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    );

    protected abstract AlgorithmFactory<ALGO, CONFIG> algorithmFactory();

    protected ComputationResult<ALGO, ALGO_RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        ProcPreconditions.check();
        return compute(graphNameOrConfig, configuration, true, true);
    }

    protected ComputationResult<ALGO, ALGO_RESULT, CONFIG> compute(
        Object graphNameOrConfig,
        Map<String, Object> configuration,
        boolean releaseAlgorithm,
        boolean releaseTopology
    ) {
        return procedureExecutor().compute(graphNameOrConfig, configuration, releaseAlgorithm, releaseTopology);
    }

    protected ProcedureExecutor<ALGO, ALGO_RESULT, CONFIG> procedureExecutor() {
        return new ProcedureExecutor<>(
            configParser(),
            memoryUsageValidator(),
            this::graphStoreLoader,
            getValidationConfig(),
            algorithmFactory(),
            transaction,
            log,
            taskRegistryFactory,
            procName(),
            allocationTracker()
        );
    }

    /**
     * Returns a single node property that has been produced by the procedure.
     */
    protected NodeProperties nodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        throw new UnsupportedOperationException("Procedure must implement org.neo4j.gds.AlgoBaseProc.nodeProperty");
    }

    protected Stream<MemoryEstimateResult> computeEstimate(
        Object graphNameOrConfig,
        Map<String, Object> configuration
    ) {
        Pair<CONFIG, Optional<String>> configAndGraphName = configParser().processInput(
            graphNameOrConfig,
            configuration
        );

        var config = configAndGraphName.getOne();
        var maybeGraphName = configAndGraphName.getTwo();

        MemoryTreeWithDimensions memoryTreeWithDimensions = procedureMemoryEstimation(graphStoreLoader(config, maybeGraphName)).memoryEstimation(config);
        return Stream.of(
            new MemoryEstimateResult(memoryTreeWithDimensions)
        );
    }

    public ValidationConfiguration<CONFIG> getValidationConfig() {
        return ValidationConfiguration.empty();
    }

    public Validator<CONFIG> validator() {
        return new Validator<>(getValidationConfig());
    }

    protected GraphStore getOrCreateGraphStore(CONFIG config, GraphStoreLoader graphStoreLoader) {
        Validator<CONFIG> validator = validator();

        var graphCreateConfig = graphStoreLoader.graphCreateConfig();
        validator.validateConfigsBeforeLoad(graphCreateConfig, config);
        var graphStore = graphStoreLoader.graphStore();
        validator.validateConfigWithGraphStore(graphStore, graphCreateConfig, config);

        return graphStore;
    }

    protected GraphStoreLoader graphStoreLoader(CONFIG config, Optional<String> maybeGraphName) {
        return GraphStoreLoader.of(
            config,
            maybeGraphName,
            this::databaseId,
            this::username,
            this::graphLoaderContext,
            isGdsAdmin()
        );
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
