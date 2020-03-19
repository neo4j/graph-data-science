/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.labelpropagation;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.MutateProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.labelpropagation.LabelPropagationProc.LABEL_PROPAGATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class LabelPropagationMutateProc extends MutateProc<LabelPropagation, LabelPropagation, LabelPropagationMutateProc.MutateResult, LabelPropagationMutateConfig> {

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
    protected AlgorithmFactory<LabelPropagation, LabelPropagationMutateConfig> algorithmFactory(
        LabelPropagationMutateConfig config
    ) {
        return new LabelPropagationFactory<>(config);
    }

    @Override
    protected PropertyTranslator<LabelPropagation> nodePropertyTranslator(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationMutateConfig> computationResult) {
        return LabelPropagationProc.nodePropertyTranslator(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<LabelPropagation, LabelPropagation, LabelPropagationMutateConfig> computeResult) {
        return LabelPropagationProc.resultBuilder(
            new MutateResult.Builder(computeResult.graph().nodeCount(), callContext, computeResult.tracker()),
            computeResult
        );
    }

    public static class MutateResult {

        public long createMillis;
        public long computeMillis;
        public long mutateMillis;
        public long postProcessingMillis;
        public long communityCount;
        public long ranIterations;
        public boolean didConverge;
        public Map<String, Object> communityDistribution;
        public Map<String, Object> configuration;

        MutateResult(
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long postProcessingMillis,
            long communityCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> communityDistribution,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.communityCount = communityCount;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.communityDistribution = communityDistribution;
            this.configuration = configuration;
        }

        static class Builder extends LabelPropagationProc.LabelPropagationResultBuilder<MutateResult> {

            Builder(long nodeCount, ProcedureCallContext context, AllocationTracker tracker) {
                super(context, tracker);
            }

            @Override
            protected MutateResult buildResult() {
                return new MutateResult(
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    postProcessingDuration,
                    maybeCommunityCount.orElse(-1L),
                    ranIterations,
                    didConverge,
                    communityHistogramOrNull(),
                    config.toMap()
                );
            }
        }
    }
}
