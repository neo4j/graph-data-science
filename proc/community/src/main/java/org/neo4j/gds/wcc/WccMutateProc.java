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
package org.neo4j.gds.wcc;

import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.wcc.WccProc.WCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class WccMutateProc extends MutatePropertyProc<Wcc, DisjointSetStruct, WccMutateProc.MutateResult, WccMutateConfig> {

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
    protected AbstractAlgorithmFactory<Wcc, WccMutateConfig> algorithmFactory() {
        return WccProc.algorithmFactory();
    }

    @Override
    protected NodeProperties nodeProperties(
        ComputationResult<Wcc, DisjointSetStruct, WccMutateConfig> computationResult
    ) {
        return WccProc.nodeProperties(computationResult, computationResult.config().mutateProperty(), allocationTracker());
    }

    @Override
    protected AbstractResultBuilder<WccMutateProc.MutateResult> resultBuilder(ComputationResult<Wcc, DisjointSetStruct, WccMutateConfig> computeResult) {
        return WccProc.resultBuilder(
            new MutateResult.Builder(callContext, computeResult.config().concurrency(), allocationTracker()),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static final class MutateResult extends WccStatsProc.StatsResult {

        public final long mutateMillis;
        public final long nodePropertiesWritten;

        MutateResult(
            long componentCount,
            Map<String, Object> componentDistribution,
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                componentCount,
                componentDistribution,
                createMillis,
                computeMillis,
                postProcessingMillis,
                configuration
            );
            this.mutateMillis = mutateMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends AbstractCommunityResultBuilder<WccMutateProc.MutateResult> {

            Builder(
                ProcedureCallContext context,
                int concurrency,
                AllocationTracker tracker
            ) {
                super(context, concurrency, tracker);
            }

            @Override
            protected WccMutateProc.MutateResult buildResult() {
                return new WccMutateProc.MutateResult(
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
