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

package org.neo4j.graphalgo.beta.modularity;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.WriteProc;
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

import static org.neo4j.graphalgo.beta.modularity.ModularityOptimizationProc.MODULARITY_OPTIMIZATION_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class ModularityOptimizationWriteProc extends WriteProc<ModularityOptimization, ModularityOptimization, ModularityOptimizationWriteProc.WriteResult, ModularityOptimizationWriteConfig> {

    @Procedure(name = "gds.beta.modularityOptimization.write", mode = WRITE)
    @Description(MODULARITY_OPTIMIZATION_DESCRIPTION)
    public Stream<WriteResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return write(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.beta.modularityOptimization.write.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected ModularityOptimizationWriteConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return ModularityOptimizationWriteConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<ModularityOptimization, ModularityOptimizationWriteConfig> algorithmFactory(
        ModularityOptimizationWriteConfig config
    ) {
        return new ModularityOptimizationFactory<>();
    }

    @Override
    protected PropertyTranslator<ModularityOptimization> nodePropertyTranslator(ComputationResult<ModularityOptimization, ModularityOptimization, ModularityOptimizationWriteConfig> computationResult) {
        return ModularityOptimizationProc.ModularityOptimizationTranslator.INSTANCE;
    }

    @Override
    protected AbstractResultBuilder<WriteResult> resultBuilder(ComputationResult<ModularityOptimization, ModularityOptimization, ModularityOptimizationWriteConfig> computeResult) {
        return ModularityOptimizationProc.resultBuilder(
            new WriteResult.Builder(callContext, computeResult.tracker()),
            computeResult
        );
    }

    public static class WriteResult {

        public final long createMillis;
        public final long computeMillis;
        public final long writeMillis;
        public final long postProcessingMillis;
        public final long nodes;
        public boolean didConverge;
        public long ranIterations;
        public double modularity;
        public final long communityCount;
        public final Map<String, Object> communityDistribution;
        public final Map<String, Object> configuration;

        WriteResult(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long writeMillis,
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
            this.writeMillis = writeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.nodes = nodes;
            this.didConverge = didConverge;
            this.ranIterations = ranIterations;
            this.modularity = modularity;
            this.communityCount = communityCount;
            this.communityDistribution = communityDistribution;
            this.configuration = configuration;
        }

        static class Builder extends ModularityOptimizationProc.ModularityOptimizationResultBuilder<WriteResult> {

            Builder(
                ProcedureCallContext context,
                AllocationTracker tracker
            ) {
                super(context, tracker);
            }

            @Override
            protected WriteResult buildResult() {
                return new WriteResult(
                    createMillis,
                    computeMillis,
                    postProcessingDuration,
                    writeMillis,
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
