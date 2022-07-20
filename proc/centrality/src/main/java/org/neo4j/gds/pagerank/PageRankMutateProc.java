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
package org.neo4j.gds.pagerank;

import org.jetbrains.annotations.Nullable;
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

@GdsCallable(name = "gds.pageRank.mutate", description = PageRankProc.PAGE_RANK_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class PageRankMutateProc extends MutatePropertyProc<PageRankAlgorithm, PageRankResult, PageRankMutateProc.MutateResult, PageRankMutateConfig> {

    @Procedure(value = "gds.pageRank.mutate", mode = READ)
    @Description(PageRankProc.PAGE_RANK_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<PageRankAlgorithm, PageRankResult, PageRankMutateConfig> computationResult = compute(
            graphName,
            configuration
        );
        return mutate(computationResult);
    }

    @Procedure(value = "gds.pageRank.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected PageRankMutateConfig newConfig(String username, CypherMapWrapper config) {
        return PageRankMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<PageRankAlgorithm, PageRankMutateConfig> algorithmFactory() {
        return new PageRankAlgorithmFactory<>();
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<PageRankAlgorithm, PageRankResult, PageRankMutateConfig> computationResult) {
        return PageRankProc.nodeProperties(computationResult);
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<PageRankAlgorithm, PageRankResult, PageRankMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return PageRankProc.resultBuilder(
            new MutateResult.Builder(executionContext.callContext(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static final class MutateResult extends PageRankStatsProc.StatsResult {

        public final long mutateMillis;
        public final long nodePropertiesWritten;

        MutateResult(
            long ranIterations,
            boolean didConverge,
            @Nullable Map<String, Object> centralityDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                ranIterations,
                didConverge,
                centralityDistribution,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                configuration
            );
            this.mutateMillis = mutateMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }


        static class Builder extends PageRankProc.PageRankResultBuilder<MutateResult> {

            Builder(ProcedureCallContext context, int concurrency) {
                super(context, concurrency);
            }

            @Override
            public MutateResult buildResult() {
                return new MutateResult(
                    ranIterations,
                    didConverge,
                    centralityHistogram,
                    preProcessingMillis,
                    computeMillis,
                    postProcessingMillis,
                    mutateMillis,
                    nodePropertiesWritten,
                    config.toMap()
                );
            }
        }
    }
}
