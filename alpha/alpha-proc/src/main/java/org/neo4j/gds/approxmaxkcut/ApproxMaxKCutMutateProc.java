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
package org.neo4j.gds.approxmaxkcut;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.MaxKCutResult;
import org.neo4j.gds.impl.approxmaxkcut.config.ApproxMaxKCutMutateConfig;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardMutateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut.APPROX_MAX_K_CUT_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.alpha.maxkcut.mutate", description = APPROX_MAX_K_CUT_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ApproxMaxKCutMutateProc extends MutatePropertyProc<ApproxMaxKCut, MaxKCutResult, ApproxMaxKCutMutateProc.MutateResult, ApproxMaxKCutMutateConfig> {

    @Procedure(value = "gds.alpha.maxkcut.mutate", mode = READ)
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphName, configuration));
    }

    @Procedure(value = "gds.alpha.maxkcut.mutate.estimate", mode = READ)
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected ApproxMaxKCutMutateConfig newConfig(String username, CypherMapWrapper config) {
        return ApproxMaxKCutMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ApproxMaxKCut, ApproxMaxKCutMutateConfig> algorithmFactory() {
        return ApproxMaxKCutProc.algorithmFactory();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<ApproxMaxKCut, MaxKCutResult, ApproxMaxKCutMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return new MutateResult.Builder(computeResult.result().cutCost());
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<ApproxMaxKCut, MaxKCutResult, ApproxMaxKCutMutateConfig> computationResult) {
        return ApproxMaxKCutProc.nodeProperties(computationResult);
    }

    public static final class MutateResult extends StandardMutateResult {

        public final long nodePropertiesWritten;
        public final double cutCost;

        MutateResult(
            long nodePropertiesWritten,
            double cutCost,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
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
            this.cutCost = cutCost;
        }

        static final class Builder extends AbstractResultBuilder<MutateResult> {

            private final double cutCost;

            Builder(double cutCost) {
                this.cutCost = cutCost;
            }

            @Override
            public MutateResult build() {
                return new MutateResult(
                    nodePropertiesWritten,
                    cutCost,
                    preProcessingMillis,
                    computeMillis,
                    0L,
                    mutateMillis,
                    config.toMap()
                );
            }
        }
    }
}
