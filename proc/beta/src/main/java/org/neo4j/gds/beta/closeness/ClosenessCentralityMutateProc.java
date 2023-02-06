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
package org.neo4j.gds.beta.closeness;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.validation.ValidationConfiguration;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardMutateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.beta.closeness.ClosenessCentralityProc.DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.closeness.mutate", description = DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ClosenessCentralityMutateProc extends MutatePropertyProc<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityMutateProc.MutateResult, ClosenessCentralityMutateConfig> {

    @Override
    public String name() {
        return "ClosenessCentrality";
    }

    @Procedure(value = "gds.beta.closeness.mutate", mode = READ)
    @Description(ClosenessCentralityProc.DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphName, configuration));
    }

    @Override
    protected ClosenessCentralityMutateConfig newConfig(String username, CypherMapWrapper config) {
        return ClosenessCentralityMutateConfig.of(config);
    }

    @Override
    public ValidationConfiguration<ClosenessCentralityMutateConfig> validationConfig(ExecutionContext executionContext) {
        return ClosenessCentralityProc.getValidationConfig();
    }

    @Override
    public GraphAlgorithmFactory<ClosenessCentrality, ClosenessCentralityMutateConfig> algorithmFactory() {
        return ClosenessCentralityProc.algorithmFactory();
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityMutateConfig> computationResult) {
        return ClosenessCentralityProc.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<ClosenessCentrality, ClosenessCentralityResult, ClosenessCentralityMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        var procResultBuilder = new MutateResult.Builder(
            executionContext.returnColumns(),
            computeResult.config().concurrency()
        ).withMutateProperty(computeResult.config().mutateProperty());
        return ClosenessCentralityProc
            .resultBuilder(procResultBuilder, computeResult);
    }

    @SuppressWarnings("unused")
    public static final class MutateResult extends StandardMutateResult {

        public final long nodePropertiesWritten;
        public final String mutateProperty;
        public final Map<String, Object> centralityDistribution;

        MutateResult(
            long nodePropertiesWritten,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            String mutateProperty,
            @Nullable Map<String, Object> centralityDistribution,
            Map<String, Object> config
        ) {
            super(preProcessingMillis, computeMillis, postProcessingMillis, mutateMillis, config);
            this.mutateProperty = mutateProperty;
            this.centralityDistribution = centralityDistribution;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static final class Builder extends AbstractCentralityResultBuilder<ClosenessCentralityMutateProc.MutateResult> {
            public String mutateProperty;

            private Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            public ClosenessCentralityMutateProc.MutateResult.Builder withMutateProperty(String mutateProperty) {
                this.mutateProperty = mutateProperty;
                return this;
            }

            @Override
            public ClosenessCentralityMutateProc.MutateResult buildResult() {
                return new ClosenessCentralityMutateProc.MutateResult(
                    nodePropertiesWritten,
                    preProcessingMillis,
                    computeMillis,
                    postProcessingMillis,
                    mutateMillis,
                    mutateProperty,
                    centralityHistogram,
                    config.toMap()
                );
            }
        }
    }
}
