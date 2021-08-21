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

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCut;
import org.neo4j.gds.impl.approxmaxkcut.ApproxMaxKCutMutateConfig;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardMutateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.approxmaxkcut.ApproxMaxKCutProc.APPROX_MAX_K_CUT_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ApproxMaxKCutMutateProc extends MutatePropertyProc<ApproxMaxKCut, ApproxMaxKCut.CutResult, ApproxMaxKCutMutateProc.MutateResult, ApproxMaxKCutMutateConfig> {

    @Procedure(value = "gds.alpha.maxkcut.mutate", mode = READ)
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.alpha.maxkcut.mutate.estimate", mode = READ)
    @Description(APPROX_MAX_K_CUT_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected ApproxMaxKCutMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ApproxMaxKCutMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ApproxMaxKCut, ApproxMaxKCutMutateConfig> algorithmFactory() {
        return ApproxMaxKCutProc.algorithmFactory();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<ApproxMaxKCut, ApproxMaxKCut.CutResult, ApproxMaxKCutMutateConfig> computeResult) {
        return new MutateResult.Builder(computeResult.result().cutCost());
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<ApproxMaxKCut, ApproxMaxKCut.CutResult, ApproxMaxKCutMutateConfig> computationResult) {
        return ApproxMaxKCutProc.nodeProperties(computationResult);
    }

    public static final class MutateResult extends StandardMutateResult {

        public final long nodePropertiesWritten;
        public final double cutCost;

        MutateResult(
            long nodePropertiesWritten,
            double cutCost,
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            Map<String, Object> config
        ) {
            super(
                createMillis,
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
                    createMillis,
                    computeMillis,
                    0L,
                    mutateMillis,
                    config.toMap()
                );
            }
        }
    }
}
