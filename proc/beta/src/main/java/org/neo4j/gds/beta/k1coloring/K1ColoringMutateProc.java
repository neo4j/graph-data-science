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
package org.neo4j.gds.beta.k1coloring;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.executor.ExecutionMode.MUTATE_NODE_PROPERTY;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.beta.k1coloring.mutate", description = K1ColoringProc.K1_COLORING_DESCRIPTION, executionMode = MUTATE_NODE_PROPERTY)
public class K1ColoringMutateProc extends MutatePropertyProc<K1Coloring, HugeLongArray, K1ColoringMutateProc.MutateResult, K1ColoringMutateConfig> {

    @Procedure(value = "gds.beta.k1coloring.mutate", mode = READ)
    @Description(K1ColoringProc.K1_COLORING_DESCRIPTION)
    public Stream<MutateResult> mutate(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphName, configuration));
    }

    @Procedure(value = "gds.beta.k1coloring.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected K1ColoringMutateConfig newConfig(String username, CypherMapWrapper config) {
        return K1ColoringMutateConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<K1Coloring, K1ColoringMutateConfig> algorithmFactory() {
        return new K1ColoringFactory<>();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(
        ComputationResult<K1Coloring, HugeLongArray, K1ColoringMutateConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return K1ColoringProc.resultBuilder(new MutateResult.Builder(
            executionContext.returnColumns(),
            computeResult.config().concurrency()
        ), computeResult, executionContext.returnColumns());
    }

    @Override
    protected NodePropertyValues nodeProperties(ComputationResult<K1Coloring, HugeLongArray, K1ColoringMutateConfig> computationResult) {
        return K1ColoringProc.nodeProperties(computationResult);
    }

    @SuppressWarnings("unused")
    public static class MutateResult {

        public static final MutateResult EMPTY = new MutateResult(
            0,
            0,
            0,
            0,
            0,
            0,
            false,
            null
        );

        public final long preProcessingMillis;
        public final long computeMillis;
        public final long mutateMillis;

        public final long nodeCount;
        public final long colorCount;
        public final long ranIterations;
        public final boolean didConverge;

        public Map<String, Object> configuration;

        MutateResult(
            long preProcessingMillis,
            long computeMillis,
            long mutateMillis,
            long nodeCount,
            long colorCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> configuration
        ) {
            this.preProcessingMillis = preProcessingMillis;
            this.computeMillis = computeMillis;
            this.mutateMillis = mutateMillis;
            this.nodeCount = nodeCount;
            this.colorCount = colorCount;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.configuration = configuration;
        }

        static class Builder extends K1ColoringProc.K1ColoringResultBuilder<MutateResult> {
            Builder(
                ProcedureReturnColumns returnColumns,
                int concurrency
            ) {
                super(returnColumns, concurrency);
            }

            @Override
            protected MutateResult buildResult() {
                return new MutateResult(
                    preProcessingMillis,
                    computeMillis,
                    mutateMillis,
                    nodeCount,
                    colorCount,
                    ranIterations,
                    didConverge,
                    config.toMap()
                );
            }
        }
    }

}
