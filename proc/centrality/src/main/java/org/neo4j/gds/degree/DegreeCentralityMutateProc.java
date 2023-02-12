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
package org.neo4j.gds.degree;

import org.jetbrains.annotations.Nullable;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractCentralityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardMutateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.degree.mutate", description = DegreeCentralityProc.DEGREE_CENTRALITY_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class DegreeCentralityMutateProc extends MutatePropertyProc<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityMutateProc.MutateResult, DegreeCentralityMutateConfig> {

    @Procedure(value = "gds.degree.mutate", mode = READ)
    @Description(DegreeCentralityProc.DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphName, configuration));
    }

    @Procedure(value = "gds.degree.mutate.estimate", mode = READ)
    @Description(DegreeCentralityProc.DEGREE_CENTRALITY_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected DegreeCentralityMutateConfig newConfig(String username, CypherMapWrapper config) {
        return DegreeCentralityMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<DegreeCentrality, DegreeCentralityMutateConfig> algorithmFactory() {
        return new DegreeCentralityFactory<>();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return DegreeCentralityProc.resultBuilder(
            new MutateResult.Builder(executionContext.returnColumns(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<DegreeCentrality, DegreeCentrality.DegreeFunction, DegreeCentralityMutateConfig> computationResult) {
        return DegreeCentralityProc.nodeProperties(computationResult);
    }

    @SuppressWarnings("unused")
    public static final class MutateResult extends StandardMutateResult {

        public final long nodePropertiesWritten;
        public final Map<String, Object> centralityDistribution;

        MutateResult(
            long nodePropertiesWritten,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            @Nullable Map<String, Object> centralityDistribution,
            Map<String, Object> config
        ) {
            super(
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                mutateMillis,
                config
            );
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.centralityDistribution = centralityDistribution;
        }

        static final class Builder extends AbstractCentralityResultBuilder<MutateResult> {

            Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            @Override
            public MutateResult buildResult() {
                return new MutateResult(
                    nodePropertiesWritten,
                    preProcessingMillis,
                    computeMillis,
                    postProcessingMillis,
                    mutateMillis,
                    centralityHistogram,
                    config.toMap()
                );
            }
        }
    }
}
