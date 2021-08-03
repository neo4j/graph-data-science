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
package org.neo4j.graphalgo.beta.modularity;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.beta.modularity.ModularityOptimization;
import org.neo4j.gds.beta.modularity.ModularityOptimizationFactory;
import org.neo4j.gds.beta.modularity.ModularityOptimizationMutateConfig;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphalgo.MutatePropertyProc;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.beta.modularity.ModularityOptimizationProc.MODULARITY_OPTIMIZATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class ModularityOptimizationMutateProc extends MutatePropertyProc<ModularityOptimization, ModularityOptimization, ModularityOptimizationMutateProc.MutateResult, ModularityOptimizationMutateConfig> {

    @Procedure(value = "gds.beta.modularityOptimization.mutate", mode = READ)
    @Description(MODULARITY_OPTIMIZATION_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.beta.modularityOptimization.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> mutateEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected NodeProperties nodeProperties(
        ComputationResult<ModularityOptimization, ModularityOptimization, ModularityOptimizationMutateConfig> computationResult
    ) {
        return ModularityOptimizationProc.nodeProperties(computationResult, allocationTracker());
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<ModularityOptimization, ModularityOptimization, ModularityOptimizationMutateConfig> computeResult
    ) {
        return ModularityOptimizationProc.resultBuilder(
            new MutateResult.Builder(callContext, computeResult.config().concurrency(), allocationTracker()),
            computeResult
        );
    }

    @Override
    protected ModularityOptimizationMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ModularityOptimizationMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ModularityOptimization, ModularityOptimizationMutateConfig> algorithmFactory() {
        return new ModularityOptimizationFactory<>();
    }

    @SuppressWarnings("unused")
    public static class MutateResult {

        public final long createMillis;
        public final long computeMillis;
        public final long mutateMillis;
        public final long postProcessingMillis;
        public final long nodes;
        public boolean didConverge;
        public long ranIterations;
        public double modularity;
        public final long communityCount;
        public final Map<String, Object> communityDistribution;
        public final Map<String, Object> configuration;

        MutateResult(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            long nodes,
            boolean didConverge,
            long ranIterations,
            double modularity,
            long communityCount,
            Map<String, Object> communityDistribution,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodes = nodes;
            this.didConverge = didConverge;
            this.ranIterations = ranIterations;
            this.modularity = modularity;
            this.communityCount = communityCount;
            this.communityDistribution = communityDistribution;
            this.configuration = configuration;
        }

        static class Builder extends ModularityOptimizationProc.ModularityOptimizationResultBuilder<MutateResult> {

            Builder(ProcedureCallContext context, int concurrency, AllocationTracker tracker) {
                super(context, concurrency, tracker);
            }

            @Override
            protected MutateResult buildResult() {
                return new MutateResult(
                    createMillis,
                    computeMillis,
                    postProcessingDuration,
                    mutateMillis,
                    nodeCount,
                    didConverge,
                    ranIterations,
                    modularity,
                    maybeCommunityCount.orElse(0),
                    communityHistogramOrNull(),
                    config.toMap()
                );
            }
        }
    }
}
