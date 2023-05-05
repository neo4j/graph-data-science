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
package org.neo4j.gds.executor;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.executor.validation.ValidationConfiguration;

import java.util.Map;

public interface AlgorithmSpec<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig,
    RESULT,
    ALGO_FACTORY extends AlgorithmFactory<?, ALGO, CONFIG>
> {
    String name();

    ALGO_FACTORY algorithmFactory(ExecutionContext executionContext);

    /**
     * This is used to enhance the user input, mainly when running ML pipelines and the user has provided pre-trained model.
     *
     * @param userInput        - user provided configuration
     * @param executionContext - the execution context provided during a procedure execution
     * @return an enhanced configuration map
     */
    default Map<String, Object> preProcessConfig(Map<String, Object> userInput, ExecutionContext executionContext) {
        return userInput;
    }

    NewConfigFunction<CONFIG> newConfigFunction();

    ComputationResultConsumer<ALGO, ALGO_RESULT, CONFIG, RESULT> computationResultConsumer();

    default ValidationConfiguration<CONFIG> validationConfig(ExecutionContext executionContext) {
        return ValidationConfiguration.empty();
    }

    // TODO: Remove this and use the ModelCatalog from the ExecutionContext
    default AlgorithmSpec<ALGO, ALGO_RESULT, CONFIG, RESULT, ALGO_FACTORY> withModelCatalog(ModelCatalog modelCatalog) {
        return this;
    }

    default ProcedureExecutorSpec<ALGO, ALGO_RESULT, CONFIG> createDefaultExecutorSpec() {
        return new ProcedureExecutorSpec<>();
    }

    default boolean releaseProgressTask() {
        return true;
    }
}
