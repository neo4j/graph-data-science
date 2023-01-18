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

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.AlgoConfigParser;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutorSpec;
import org.neo4j.gds.executor.GraphCreationFactory;
import org.neo4j.gds.executor.GraphStoreFromCatalogLoader;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.MemoryUsageValidator;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.executor.ProcConfigParser;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.executor.ProcedureExecutorSpec;
import org.neo4j.gds.executor.ProcedureGraphCreationFactory;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.executor.validation.Validator;
import org.neo4j.gds.results.MemoryEstimateResult;

import java.util.Map;
import java.util.stream.Stream;

public abstract class AlgoBaseProc<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig,
    PROC_RESULT
> extends BaseProc implements AlgorithmSpec<ALGO, ALGO_RESULT, CONFIG, Stream<PROC_RESULT>, AlgorithmFactory<?, ALGO, CONFIG>> {

    public static final String STATS_DESCRIPTION = "Executes the algorithm and returns result statistics without writing the result to Neo4j.";

    public ProcConfigParser<CONFIG> configParser() {
        var defaults = DefaultsConfiguration.Instance;
        var limits = LimitsConfiguration.Instance;

        return new AlgoConfigParser<>(username(), AlgoBaseProc.this::newConfig, defaults, limits);
    }

    protected abstract CONFIG newConfig(
        String username,
        CypherMapWrapper config
    );

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
        return procedureExecutor().compute(graphName, configuration, releaseAlgorithm);
    }

    /**
     * Returns a single node property that has been produced by the procedure.
     */
    protected NodePropertyValues nodeProperties(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult) {
        throw new UnsupportedOperationException("Procedure must implement org.neo4j.gds.AlgoBaseProc.nodeProperty");
    }

    protected Stream<MemoryEstimateResult> computeEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algoConfiguration
    ) {
        return new MemoryEstimationExecutor<>(
            this,
            new ProcedureExecutorSpec<>(),
            executionContext()
        ).computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    public String name() {
        return this.getClass().getSimpleName();
    }

    @Override
    public NewConfigFunction<CONFIG> newConfigFunction() {
        return this::newConfig;
    }

    @Override
    public ValidationConfiguration<CONFIG> validationConfig() {
        return ValidationConfiguration.empty();
    }

    private ProcedureExecutor<ALGO, ALGO_RESULT, CONFIG, ComputationResult<ALGO, ALGO_RESULT, CONFIG>> procedureExecutor() {
        var pipelineSpec = new AlgoBaseExecutorSpec();

        var name = name();
        var factory = algorithmFactory();
        var configFunction = newConfigFunction();
        var validationConfig = validationConfig();
        var algoSpec = new AlgorithmSpec<ALGO, ALGO_RESULT, CONFIG, ComputationResult<ALGO, ALGO_RESULT, CONFIG>, AlgorithmFactory<?, ALGO, CONFIG>>() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public AlgorithmFactory<?, ALGO, CONFIG> algorithmFactory() {
                return factory;
            }

            @Override
            public NewConfigFunction<CONFIG> newConfigFunction() {
                return configFunction;
            }

            @Override
            public ComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, ComputationResult<ALGO, ALGO_RESULT, CONFIG>> computationResultConsumer() {
                return ComputationResultConsumer.identity();
            }

            @Override
            public ValidationConfiguration<CONFIG> validationConfig() {
                return validationConfig;
            }
        };

        return new ProcedureExecutor<>(
            algoSpec,
            pipelineSpec,
            executionContext()
        );
    }

    private final class AlgoBaseExecutorSpec implements ExecutorSpec<ALGO, ALGO_RESULT, CONFIG> {
        @Override
        public ProcConfigParser<CONFIG> configParser(
            NewConfigFunction<CONFIG> newConfigFunction, ExecutionContext executionContext
        ) {
            return AlgoBaseProc.this.configParser();
        }

        @Override
        public Validator<CONFIG> validator(ValidationConfiguration<CONFIG> validationConfiguration) {
            return new Validator<>(validationConfiguration);
        }

        @Override
        public GraphCreationFactory<ALGO, ALGO_RESULT, CONFIG> graphCreationFactory(ExecutionContext executionContext) {
            return new ProcedureGraphCreationFactory<>(
                (config, graphName) -> new GraphStoreFromCatalogLoader(
                    graphName,
                    config,
                    executionContext.username(),
                    executionContext.databaseId(),
                    executionContext.isGdsAdmin()
                ),
                new MemoryUsageValidator(executionContext.log(), executionContext.databaseService())
            );
        }
    }
}
