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
package org.neo4j.graphalgo.beta.k1coloring;

import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.beta.k1coloring.K1Coloring;
import org.neo4j.gds.beta.k1coloring.K1ColoringFactory;
import org.neo4j.gds.beta.k1coloring.K1ColoringStatsConfig;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.graphalgo.StatsProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.paged.HugeLongArray;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.graphalgo.beta.k1coloring.K1ColoringProc.K1_COLORING_DESCRIPTION;
import static org.neo4j.procedure.Mode.READ;

public class K1ColoringStatsProc extends StatsProc<K1Coloring, HugeLongArray, K1ColoringStatsProc.StatsResult, K1ColoringStatsConfig> {

    @Procedure(name = "gds.beta.k1coloring.stats", mode = READ)
    @Description(K1_COLORING_DESCRIPTION)
    public Stream<StatsResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<K1Coloring, HugeLongArray, K1ColoringStatsConfig> computationResult =
            compute(graphNameOrConfig, configuration);

        return stats(computationResult);
    }

    @Procedure(value = "gds.beta.k1coloring.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(ComputationResult<K1Coloring, HugeLongArray, K1ColoringStatsConfig> computeResult) {
        StatsResult.Builder builder = new StatsResult.Builder(
            callContext,
            computeResult.config().concurrency(),
            allocationTracker()
        );
        return K1ColoringProc.resultBuilder(builder, computeResult, callContext);
    }

    @Override
    protected AlgorithmFactory<K1Coloring, K1ColoringStatsConfig> algorithmFactory() {
        return new K1ColoringFactory<>();
    }

    @Override
    protected K1ColoringStatsConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return K1ColoringStatsConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @SuppressWarnings("unused")
    public static class StatsResult {

        public final long createMillis;
        public final long computeMillis;

        public final long nodeCount;
        public final long colorCount;
        public final long ranIterations;
        public final boolean didConverge;

        public Map<String, Object> configuration;

        StatsResult(
            long createMillis,
            long computeMillis,
            long nodeCount,
            long colorCount,
            long ranIterations,
            boolean didConverge,
            Map<String, Object> configuration
        ) {
            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.nodeCount = nodeCount;
            this.colorCount = colorCount;
            this.ranIterations = ranIterations;
            this.didConverge = didConverge;
            this.configuration = configuration;
        }

        static class Builder extends K1ColoringProc.K1ColoringResultBuilder<StatsResult> {

            Builder(
                ProcedureCallContext context,
                int concurrency,
                AllocationTracker tracker
            ) {
                super(context, concurrency, tracker);
            }

            @Override
            protected StatsResult buildResult() {
                return new StatsResult(
                    createMillis,
                    computeMillis,
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
