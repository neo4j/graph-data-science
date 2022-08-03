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
package org.neo4j.gds.modularity;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.modularityOptimization.mutate", description = ModularityOptimizationProc.MODULARITY_OPTIMIZATION_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class ModularityOptimizationMutateProc extends MutatePropertyProc<ModularityOptimization, ModularityOptimization, ModularityOptimizationMutateProc.MutateResult, ModularityOptimizationMutateConfig> {

    @Procedure(value = "gds.beta.modularityOptimization.mutate", mode = READ)
    @Description(ModularityOptimizationProc.MODULARITY_OPTIMIZATION_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphName, configuration));
    }

    @Procedure(value = "gds.beta.modularityOptimization.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected NodePropertyValues nodeProperties(
        ComputationResult<ModularityOptimization, ModularityOptimization, ModularityOptimizationMutateConfig> computationResult
    ) {
        return ModularityOptimizationProc.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<ModularityOptimization, ModularityOptimization, ModularityOptimizationMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return ModularityOptimizationProc.resultBuilder(
            new MutateResult.Builder(executionContext.callContext(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @Override
    protected ModularityOptimizationMutateConfig newConfig(String username, CypherMapWrapper config) {
        return ModularityOptimizationMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<ModularityOptimization, ModularityOptimizationMutateConfig> algorithmFactory() {
        return new ModularityOptimizationFactory<>();
    }

    @SuppressWarnings("unused")
    public static class MutateResult {

        public final long preProcessingMillis;
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
            long preProcessingMillis,
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
            this.preProcessingMillis = preProcessingMillis;
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

            Builder(ProcedureCallContext context, int concurrency) {
                super(context, concurrency);
            }

            @Override
            protected MutateResult buildResult() {
                return new MutateResult(
                    preProcessingMillis,
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
