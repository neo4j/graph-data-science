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

import org.neo4j.gds.AbstractAlgorithmFactory;
import org.neo4j.gds.MutatePropertyProc;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.paged.HugeLongArray;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class K1ColoringMutateProc extends MutatePropertyProc<K1Coloring, HugeLongArray, K1ColoringMutateProc.MutateResult, K1ColoringMutateConfig> {

    @Procedure(value = "gds.beta.k1coloring.mutate", mode = READ)
    @Description(K1ColoringProc.K1_COLORING_DESCRIPTION)
    public Stream<K1ColoringMutateProc.MutateResult> mutate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.beta.k1coloring.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> mutateEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected K1ColoringMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return K1ColoringMutateConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AbstractAlgorithmFactory<K1Coloring, K1ColoringMutateConfig> algorithmFactory() {
        return new K1ColoringFactory<>();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<K1Coloring, HugeLongArray, K1ColoringMutateConfig> computeResult) {
        return K1ColoringProc.resultBuilder(new MutateResult.Builder(callContext, computeResult.config().concurrency(), allocationTracker()), computeResult, callContext);
    }

    @Override
    protected NodeProperties nodeProperties(ComputationResult<K1Coloring, HugeLongArray, K1ColoringMutateConfig> computationResult) {
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

        public final long createMillis;
        public final long computeMillis;
        public final long mutateMillis;

        public final long nodeCount;
        public final long colorCount;
        public final long ranIterations;
        public final boolean didConverge;

        public Map<String, Object> configuration;

        MutateResult(
            long createMillis,
            long computeMillis,
            long mutateMillis,
            long nodeCount,
            long colorCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
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
                ProcedureCallContext context,
                int concurrency,
                AllocationTracker tracker
            ) {
                super(context, concurrency, tracker);
            }

            @Override
            protected MutateResult buildResult() {
                return new MutateResult(
                    createMillis,
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
