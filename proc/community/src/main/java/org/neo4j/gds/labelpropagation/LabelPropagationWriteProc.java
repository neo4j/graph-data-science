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

import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.WriteProc;
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

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class LabelPropagationWriteProc extends WriteProc<LabelPropagation, LabelPropagation, LabelPropagationWriteProc.WriteResult, LabelPropagationWriteConfig> {

    @Procedure(value = "gds.labelPropagation.write", mode = WRITE)
    @Description(LabelPropagationProc.LABEL_PROPAGATION_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.labelPropagation.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationWriteConfig> computationResult) {
        return LabelPropagationProc.nodeProperties(computationResult, computationResult.config().writeProperty(), allocationTracker());
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationWriteConfig> computeResult) {
        return LabelPropagationProc.resultBuilder(
            new WriteResult.Builder(callContext, computeResult.config().concurrency(), allocationTracker()),
            computeResult
        );
    }

    @Override
    public LabelPropagationWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LabelPropagationWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AbstractAlgorithmFactory<LabelPropagation, LabelPropagationWriteConfig> algorithmFactory() {
        return new LabelPropagationFactory<>();
    }

    @SuppressWarnings("unused")
    public static class WriteResult extends LabelPropagationStatsProc.StatsResult {

        public final long writeMillis;
        public final long nodePropertiesWritten;

        WriteResult(
            long ranIterations,
            boolean didConverge,
            long communityCount,
            Map<String, Object> communityDistribution,
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
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
            this.writeMillis = writeMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }


        static class Builder extends LabelPropagationProc.LabelPropagationResultBuilder<WriteResult> {

            Builder(ProcedureCallContext context, int concurrency, AllocationTracker tracker) {
                super(context, concurrency, tracker);
            }

            @Override
            protected WriteResult buildResult() {
                return new WriteResult(
                    ranIterations,
                    didConverge,
                    maybeCommunityCount.orElse(0L),
                    communityHistogramOrNull(),
                    createMillis,
                    computeMillis,
                    postProcessingDuration,
                    writeMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }

}
