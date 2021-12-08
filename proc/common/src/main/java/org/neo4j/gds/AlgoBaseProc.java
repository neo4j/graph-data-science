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
import org.neo4j.gds.pipeline.ComputationResultConsumer;
import org.neo4j.gds.pipeline.GraphCreationFactory;
import org.neo4j.gds.pipeline.MemoryEstimtationExecutor;
import org.neo4j.gds.pipeline.ProcedureGraphCreationFactory;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.validation.ValidationConfiguration;
import org.neo4j.gds.validation.Validator;

import java.util.Map;
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
        return new AlgoConfigParser<>(username(), AlgoBaseProc.this::newConfig);
    }

    protected abstract CONFIG newConfig(
        String username,
        CypherMapWrapper config
    );

    protected abstract AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory();

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
        return new MemoryEstimtationExecutor<>(
            configParser(),
            algorithmFactory(),
            this::graphLoaderContext,
            username(),
            this::databaseId,
            isGdsAdmin()
        ).computeEstimate(graphNameOrConfiguration, algoConfiguration);

    }

    public ValidationConfiguration<CONFIG> getValidationConfig() {
        return ValidationConfiguration.empty();
    }

    protected Validator<CONFIG> validator() {
        return new Validator<>(getValidationConfig());
    }

    private ProcedureExecutor<ALGO, ALGO_RESULT, CONFIG, ComputationResult<ALGO, ALGO_RESULT, CONFIG>> procedureExecutor() {
        return new ProcedureExecutor<>(
            configParser(),
            validator(),
            algorithmFactory(),
            transaction,
            log,
            taskRegistryFactory,
            procName(),
            allocationTracker(),
            ComputationResultConsumer.identity(),
            graphCreationFactory()
        );
    }

    protected GraphCreationFactory<ALGO, ALGO_RESULT, CONFIG> graphCreationFactory() {
        return new ProcedureGraphCreationFactory<>(
            (config, graphName) -> new GraphStoreFromCatalogLoader(
                graphName,
                config,
                username(),
                databaseId(),
                isGdsAdmin()
            ),
            memoryUsageValidator()
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
