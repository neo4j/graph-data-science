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
package org.neo4j.graphalgo.louvain;

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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.louvain.LouvainProc.LOUVAIN_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class LouvainMutateProc extends MutateProc<Louvain, Louvain, LouvainMutateProc.MutateResult, LouvainMutateConfig> {

    @Procedure(value = "gds.louvain.mutate", mode = WRITE)
    @Description(LOUVAIN_DESCRIPTION)
    public Stream<MutateResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.louvain.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected LouvainMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return LouvainMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Louvain, LouvainMutateConfig> algorithmFactory(LouvainMutateConfig config) {
        return new LouvainFactory<>();
    }

    @Override
    protected PropertyTranslator<Louvain> nodePropertyTranslator(ComputationResult<Louvain, Louvain, LouvainMutateConfig> computationResult) {
        return LouvainProc.nodePropertyTranslator(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<Louvain, Louvain, LouvainMutateConfig> computeResult) {
        return LouvainProc.resultBuilder(
            new MutateResult.Builder(callContext, computeResult.tracker()),
            computeResult
        );
    }

    public static final class MutateResult {

        public long createMillis;
        public long computeMillis;
        public long mutateMillis;
        public long postProcessingMillis;
        public long ranLevels;
        public long communityCount;
        public double modularity;
        public List<Double> modularities;
        public Map<String, Object> communityDistribution;
        public Map<String, Object> configuration;

        MutateResult(
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long postProcessingMillis,
            long ranLevels,
            long communityCount,
            double modularity,
            double[] modularities,
            Map<String, Object> communityDistribution,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.ranLevels = ranLevels;
            this.communityCount = communityCount;
            this.modularity = modularity;
            this.modularities = Arrays.stream(modularities).boxed().collect(Collectors.toList());
            this.communityDistribution = communityDistribution;
            this.configuration = configuration;
        }

        static class Builder extends LouvainProc.LouvainResultBuilder<MutateResult> {

            Builder(ProcedureCallContext context, AllocationTracker tracker) {
                super(context, tracker);
            }

            @Override
            protected MutateResult buildResult() {
                return new MutateResult(
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    postProcessingDuration,
                    levels,
                    maybeCommunityCount.orElse(-1L),
                    modularity,
                    modularities,
                    communityHistogramOrNull(),
                    config.toMap()
                );
            }
        }
    }
}
