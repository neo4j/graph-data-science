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
package org.neo4j.gds.labelpropagation;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.labelpropagation.LabelPropagationProc.LABEL_PROPAGATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class LabelPropagationMutateProc extends MutatePropertyProc<LabelPropagation, LabelPropagation, LabelPropagationMutateProc.MutateResult, LabelPropagationMutateConfig> {

    @Procedure(value = "gds.labelPropagation.mutate", mode = READ)
    @Description(LABEL_PROPAGATION_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.labelPropagation.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> mutateEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected LabelPropagationMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LabelPropagationMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<LabelPropagation, LabelPropagationMutateConfig> algorithmFactory() {
        return new LabelPropagationFactory<>();
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationMutateConfig> computationResult) {
        return LabelPropagationProc.nodeProperties(computationResult, computationResult.config().mutateProperty(), allocationTracker());
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationMutateConfig> computeResult) {
        return LabelPropagationProc.resultBuilder(
            new MutateResult.Builder(callContext, computeResult.config().concurrency(), allocationTracker()),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static class MutateResult extends LabelPropagationStatsProc.StatsResult {

        public final long mutateMillis;
        public final long nodePropertiesWritten;

        MutateResult(
            long ranIterations,
            boolean didConverge,
            long communityCount,
            Map<String, Object> communityDistribution,
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                ranIterations,
                didConverge,
                communityCount,
                communityDistribution,
                createMillis,
                computeMillis,
                postProcessingMillis,
                configuration
            );
            this.mutateMillis = mutateMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends LabelPropagationProc.LabelPropagationResultBuilder<MutateResult> {

            Builder(ProcedureCallContext context, int concurrency, AllocationTracker allocationTracker) {
                super(context, concurrency, allocationTracker);
            }

            @Override
            protected MutateResult buildResult() {
                return new MutateResult(
                    ranIterations,
                    didConverge,
                    maybeCommunityCount.orElse(0L),
                    communityHistogramOrNull(),
                    createMillis,
                    computeMillis,
                    postProcessingDuration,
                    mutateMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }
}
