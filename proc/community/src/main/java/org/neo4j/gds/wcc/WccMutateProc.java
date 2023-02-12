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
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.MemoryEstimationExecutor;
import org.neo4j.gds.executor.ProcedureExecutor;
import org.neo4j.gds.executor.ProcedureExecutorSpec;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.wcc.WccProc.WCC_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class WccMutateProc extends AlgoBaseProc<Wcc, DisjointSetStruct, WccMutateConfig, WccMutateProc.MutateResult> {

    @Procedure(value = "gds.wcc.mutate", mode = READ)
    @Description(WCC_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        var mutateSpec = new WccMutateSpec();
        var pipelineSpec = new ProcedureExecutorSpec<Wcc, DisjointSetStruct, WccMutateConfig>();

        return new ProcedureExecutor<>(
            mutateSpec,
            pipelineSpec,
            executionContext()
        ).compute(graphName, configuration);
    }

    @Procedure(value = "gds.wcc.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        var mutateSpec = new WccMutateSpec();
        var pipelineSpec = new ProcedureExecutorSpec<Wcc, DisjointSetStruct, WccMutateConfig>();

        return new MemoryEstimationExecutor<>(
            mutateSpec,
            pipelineSpec,
            executionContext(),
            transactionContext()
        ).computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected WccMutateConfig newConfig(String username, CypherMapWrapper config) {
        return WccMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Wcc, WccMutateConfig> algorithmFactory() {
        return new WccAlgorithmFactory<>();
    }

    @Override
    public ComputationResultConsumer<Wcc, DisjointSetStruct, WccMutateConfig, Stream<MutateResult>> computationResultConsumer() {
        return new WccMutateSpec().computationResultConsumer();
    }

    @SuppressWarnings("unused")
    public static final class MutateResult extends WccStatsProc.StatsResult {

        public final long mutateMillis;
        public final long nodePropertiesWritten;

        MutateResult(
            long componentCount,
            Map<String, Object> componentDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                componentCount,
                componentDistribution,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                configuration
            );
            this.mutateMillis = mutateMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends AbstractCommunityResultBuilder<MutateResult> {

            Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            @Override
            protected MutateResult buildResult() {
                return new MutateResult(
                    maybeCommunityCount.orElse(0L),
                    communityHistogramOrNull(),
                    preProcessingMillis,
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
