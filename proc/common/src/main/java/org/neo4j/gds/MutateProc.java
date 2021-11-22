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

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.config.MutateConfig;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.validation.ValidationConfiguration;
import org.neo4j.gds.validation.Validator;

import java.util.stream.Stream;

public abstract class MutateProc<
    ALGO extends Algorithm<ALGO, ALGO_RESULT>,
    ALGO_RESULT,
    PROC_RESULT,
    CONFIG extends MutateConfig> extends AlgoBaseProc<ALGO, ALGO_RESULT, CONFIG> {

    protected abstract AbstractResultBuilder<PROC_RESULT> resultBuilder(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult);

    @Override
    public Validator<CONFIG> validator() {
        return new MutateValidator<>(getValidationConfig());
    }

    protected Stream<PROC_RESULT> mutate(ComputationResult<ALGO, ALGO_RESULT, CONFIG> computeResult) {
        return runWithExceptionLogging("Graph mutation failed", () -> {
            CONFIG config = computeResult.config();

            AbstractResultBuilder<PROC_RESULT> builder = resultBuilder(computeResult)
                .withCreateMillis(computeResult.createMillis())
                .withComputeMillis(computeResult.computeMillis())
                .withNodeCount(computeResult.graph().nodeCount())
                .withConfig(config);

            if (computeResult.isGraphEmpty()) {
                return Stream.of(builder.build());
            } else {
                updateGraphStore(builder, computeResult);
                computeResult.graph().releaseProperties();
                return Stream.of(builder.build());
            }
        });
    }

    protected abstract void updateGraphStore(
        AbstractResultBuilder<?> resultBuilder,
        ComputationResult<ALGO, ALGO_RESULT, CONFIG> computationResult
    );

    private static class MutateValidator<CONFIG extends AlgoBaseConfig> extends Validator<CONFIG> {

        MutateValidator(ValidationConfiguration<CONFIG> validationConfiguration) {
            super(validationConfiguration);
        }

        @Override
        public void validateConfigsBeforeLoad(GraphCreateConfig graphCreateConfig, CONFIG config) {
            validateMutateConfig(config);
            super.validateConfigsBeforeLoad(graphCreateConfig, config);
        }

        private void validateMutateConfig(CONFIG config) {
            if (config.implicitCreateConfig().isPresent() && config instanceof MutateConfig) {
                throw new IllegalArgumentException(
                    "Cannot mutate implicitly loaded graphs. Use a loaded graph in the graph-catalog"
                );
            }
        }
    }
}
