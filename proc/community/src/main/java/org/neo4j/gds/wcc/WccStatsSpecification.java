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

import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.executor.AlgorithmSpec;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ComputationResultConsumer;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ExecutionMode;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.executor.NewConfigFunction;
import org.neo4j.gds.result.AbstractCommunityResultBuilder;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.StandardStatsResult;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.LoggingUtil.runWithExceptionLogging;

@GdsCallable(name = "gds.wcc.stats", description = "Executes the algorithm and returns result statistics without writing the result to Neo4j.", executionMode = ExecutionMode.STATS)
public class WccStatsSpecification implements AlgorithmSpec<Wcc, DisjointSetStruct, WccStatsConfig, Stream<WccStatsSpecification.StatsResult>, WccAlgorithmFactory<WccStatsConfig>> {

    @Override
    public String name() {
        return "WccStats";
    }

    @Override
    public WccAlgorithmFactory<WccStatsConfig> algorithmFactory() {
        return new WccAlgorithmFactory<>();
    }

    @Override
    public NewConfigFunction<WccStatsConfig> newConfigFunction() {
        return (__, userInput) -> WccStatsConfig.of(userInput);
    }

    @Override
    public ComputationResultConsumer<Wcc, DisjointSetStruct, WccStatsConfig, Stream<StatsResult>> computationResultConsumer() {
        return (computationResult, executionContext) -> runWithExceptionLogging(
            "Stats call failed",
            executionContext.log(),
            () -> Stream.of(
            resultBuilder(computationResult, executionContext)
                .withPreProcessingMillis(computationResult.preProcessingMillis())
                .withComputeMillis(computationResult.computeMillis())
                .withNodeCount(computationResult.graph().nodeCount())
                .withConfig(computationResult.config())
                .build()
        ));
    }

    protected AbstractResultBuilder<StatsResult> resultBuilder(
        ComputationResult<Wcc, DisjointSetStruct, WccStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        AbstractCommunityResultBuilder<StatsResult> resultBuilder = new StatsResult.Builder(
            executionContext.returnColumns(),
            computeResult.config().concurrency()
        );
        computeResult.result().ifPresent(result -> resultBuilder.withCommunityFunction(result::setIdOf));
        return resultBuilder;
    }

    @SuppressWarnings("unused")
    public static class StatsResult extends StandardStatsResult {

        public final long componentCount;
        public final Map<String, Object> componentDistribution;

        StatsResult(
            long componentCount,
            Map<String, Object> componentDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            Map<String, Object> configuration
        ) {
            super(preProcessingMillis, computeMillis, postProcessingMillis, configuration);
            this.componentCount = componentCount;
            this.componentDistribution = componentDistribution;
        }

        static class Builder extends AbstractCommunityResultBuilder<StatsResult> {

            Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            @Override
            protected StatsResult buildResult() {
                return new StatsResult(
                    maybeCommunityCount.orElse(0L),
                    communityHistogramOrNull(),
                    preProcessingMillis,
                    computeMillis,
                    postProcessingDuration,
                    config.toMap()
                );
            }
        }
    }
}
