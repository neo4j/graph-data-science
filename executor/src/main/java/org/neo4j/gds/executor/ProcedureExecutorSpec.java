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
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.executor.validation.Validator;

public class ProcedureExecutorSpec<
    ALGO extends Algorithm<ALGO_RESULT>,
    ALGO_RESULT,
    CONFIG extends AlgoBaseConfig
> implements ExecutorSpec<ALGO, ALGO_RESULT, CONFIG> {

    public ProcedureExecutorSpec() { }

    @Override
    public ProcConfigParser<CONFIG> configParser(NewConfigFunction<CONFIG> newConfigFunction, ExecutionContext executionContext) {
        var defaults = DefaultsConfiguration.Instance;
        var limits = LimitsConfiguration.Instance;

        return new AlgoConfigParser<>(executionContext.username(), newConfigFunction, defaults, limits);
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
