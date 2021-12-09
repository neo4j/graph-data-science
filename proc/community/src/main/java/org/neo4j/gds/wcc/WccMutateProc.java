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

import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.ProcedureExecutor;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.pipeline.MemoryEstimationExecutor;
import org.neo4j.gds.pipeline.ProcedurePipelineSpec;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.wcc.WccProc.WCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class WccMutateProc extends AlgoBaseProc<Wcc, DisjointSetStruct, WccMutateConfig> {

    @Procedure(value = "gds.wcc.mutate", mode = READ)
    @Description(WCC_DESCRIPTION)
    public Stream<WccMutateProc.MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var mutateSpec = new WccMutateSpec(callContext, allocationTracker(), log);
        var pipelineSpec = new ProcedurePipelineSpec<>(
            username(),
            graphCreationFactory()
        );

        return new ProcedureExecutor<>(
            pipelineSpec.configParser(mutateSpec.newConfigFunction()),
            pipelineSpec.validator(mutateSpec.validationConfig()),
            algorithmFactory(),
            transaction,
            log,
            taskRegistryFactory,
            procName(),
            allocationTracker(),
            mutateSpec.computationResultConsumer(),
            pipelineSpec.graphCreationFactory()
        ).compute(graphName, configuration, true, true);
    }

    @Procedure(value = "gds.wcc.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        var mutateSpec = new WccMutateSpec(callContext, allocationTracker(), log);
        var pipelineSpec = new ProcedurePipelineSpec<>(
            username(),
            graphCreationFactory()
        );

        return new MemoryEstimationExecutor<>(
            pipelineSpec.configParser(mutateSpec.newConfigFunction()),
            mutateSpec.algorithmFactory(),
            this::graphLoaderContext,
            this::databaseId,
            username(),
            isGdsAdmin()
        ).computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected WccMutateConfig newConfig(String username, CypherMapWrapper config) {
        return WccMutateConfig.of(config);
    }

    @Override
    protected GraphAlgorithmFactory<Wcc, WccMutateConfig> algorithmFactory() {
        return new WccAlgorithmFactory<>();
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
                AllocationTracker allocationTracker
            ) {
                super(context, concurrency, allocationTracker);
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
