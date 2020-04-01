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
package org.neo4j.graphalgo.wcc;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.MutateProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractCommunityResultBuilder;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.wcc.WccProc.WCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class WccMutateProc extends MutateProc<Wcc, DisjointSetStruct, WccMutateProc.MutateResult, WccMutateConfig> {

    @Procedure(value = "gds.wcc.mutate", mode = READ)
    @Description(WCC_DESCRIPTION)
    public Stream<WccMutateProc.MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Wcc, DisjointSetStruct, WccMutateConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );

        return mutate(computationResult);
    }

    @Procedure(value = "gds.wcc.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> mutateEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected WccMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return WccMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Wcc, WccMutateConfig> algorithmFactory(WccMutateConfig config) {
        return WccProc.algorithmFactory();
    }

    @Override
    protected PropertyTranslator<DisjointSetStruct> nodePropertyTranslator(
        ComputationResult<Wcc, DisjointSetStruct, WccMutateConfig> computationResult
    ) {
        return WccProc.nodePropertyTranslator(computationResult, computationResult.config().mutateProperty());
    }

    @Override
    protected AbstractResultBuilder<WccMutateProc.MutateResult> resultBuilder(ComputationResult<Wcc, DisjointSetStruct, WccMutateConfig> computeResult) {
        return WccProc.resultBuilder(
            new MutateResult.Builder(callContext, computeResult.tracker()),
            computeResult
        );
    }

    public static final class MutateResult {

        public final long nodePropertiesWritten;
        public final long createMillis;
        public final long computeMillis;
        public final long mutateMillis;
        public final long postProcessingMillis;
        public final long componentCount;
        public final Map<String, Object> componentDistribution;
        public final Map<String, Object> configuration;

        MutateResult(
            long nodePropertiesWritten,
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long postProcessingMillis,
            long componentCount,
            Map<String, Object> componentDistribution,
            Map<String, Object> configuration
        ) {
            this.nodePropertiesWritten = nodePropertiesWritten;
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.componentCount = componentCount;
            this.componentDistribution = componentDistribution;
            this.configuration = configuration;
        }

        static class Builder extends AbstractCommunityResultBuilder<WccMutateProc.MutateResult> {

            Builder(
                ProcedureCallContext context,
                AllocationTracker tracker
            ) {
                super(context, tracker);
            }

            @Override
            protected WccMutateProc.MutateResult buildResult() {
                return new WccMutateProc.MutateResult(
                    nodePropertiesWritten,
                    createMillis,
                    computeMillis,
                    mutateMillis,
                    postProcessingDuration,
                    maybeCommunityCount.orElse(-1L),
                    communityHistogramOrNull(),
                    config.toMap()
                );
            }
        }
    }
}
