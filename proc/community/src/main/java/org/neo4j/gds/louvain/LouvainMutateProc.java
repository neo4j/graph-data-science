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
package org.neo4j.gds.louvain;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.louvain.mutate", description = LouvainProc.LOUVAIN_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class LouvainMutateProc extends MutatePropertyProc<Louvain, LouvainResult, LouvainMutateProc.MutateResult, LouvainMutateConfig> {

    @Procedure(value = "gds.louvain.mutate", mode = READ)
    @Description(LouvainProc.LOUVAIN_DESCRIPTION)
    public Stream<MutateResult> write(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphName, configuration));
    }

    @Procedure(value = "gds.louvain.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected LouvainMutateConfig newConfig(String username, CypherMapWrapper config) {
        return LouvainMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<Louvain, LouvainMutateConfig> algorithmFactory() {
        return new LouvainFactory<>();
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<Louvain, LouvainResult, LouvainMutateConfig> computationResult) {
        return LouvainProc.nodeProperties(
            computationResult,
            computationResult.config().mutateProperty()
        );
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<Louvain, LouvainResult, LouvainMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return LouvainProc.resultBuilder(
            new MutateResult.Builder(executionContext.returnColumns(), computeResult.config().concurrency()),
            computeResult
        );
    }

    @SuppressWarnings("unused")
    public static final class MutateResult extends LouvainStatsProc.StatsResult {

        public final long mutateMillis;
        public final long nodePropertiesWritten;

        MutateResult(
            double modularity,
            List<Double> modularities,
            long ranLevels,
            long communityCount,
            Map<String, Object> communityDistribution,
            long preProcessingMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            long nodePropertiesWritten,
            Map<String, Object> configuration
        ) {
            super(
                modularity,
                modularities,
                ranLevels,
                communityCount,
                communityDistribution,
                preProcessingMillis,
                computeMillis,
                postProcessingMillis,
                configuration
            );
            this.mutateMillis = mutateMillis;
            this.nodePropertiesWritten = nodePropertiesWritten;
        }

        static class Builder extends LouvainProc.LouvainResultBuilder<MutateResult> {

            Builder(ProcedureReturnColumns returnColumns, int concurrency) {
                super(returnColumns, concurrency);
            }

            @Override
            protected MutateResult buildResult() {
                return new MutateResult(
                    modularity,
                    Arrays.stream(modularities).boxed().collect(Collectors.toList()),
                    levels,
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
