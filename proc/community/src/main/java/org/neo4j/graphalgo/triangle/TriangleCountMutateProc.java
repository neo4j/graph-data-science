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
package org.neo4j.graphalgo.triangle;

import org.jetbrains.annotations.Nullable;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.MutateProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.AllocationTracker;
import org.neo4j.graphalgo.core.write.PropertyTranslator;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;
import static org.neo4j.procedure.Mode.WRITE;

public class TriangleCountMutateProc extends MutateProc<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountMutateProc.MutateResult, TriangleCountMutateConfig> {

    @Procedure(value = "gds.triangleCount.mutate", mode = WRITE)
    @Description("")
    public Stream<MutateResult> write(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return mutate(compute(graphNameOrConfig, configuration));
    }

    @Procedure(value = "gds.triangleCount.mutate.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected void validateConfigs(
        GraphCreateConfig graphCreateConfig, TriangleCountMutateConfig config
    ) {
        TriangleCountCompanion.validateConfigs(graphCreateConfig, config);
    }

    @Override
    protected TriangleCountMutateConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return TriangleCountMutateConfig.of(
            username,
            graphName,
            maybeImplicitCreate,
            config
        );
    }

    @Override
    protected AlgorithmFactory<IntersectingTriangleCount, TriangleCountMutateConfig> algorithmFactory(
        TriangleCountMutateConfig config
    ) {
        return new IntersectingTriangleCountFactory<>();
    }

    @Override
    protected PropertyTranslator<IntersectingTriangleCount.TriangleCountResult> nodePropertyTranslator(
        ComputationResult<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountMutateConfig> computationResult
    ) {
        return TriangleCountCompanion.nodePropertyTranslator();
    }

    @Override
    protected AbstractResultBuilder<MutateResult> resultBuilder(ComputationResult<IntersectingTriangleCount, IntersectingTriangleCount.TriangleCountResult, TriangleCountMutateConfig> computeResult) {
        return TriangleCountCompanion.resultBuilder(
            new TriangleCountMutateBuilder(callContext, computeResult.tracker()),
            computeResult
        );
    }

    public static class MutateResult extends TriangleCountStatsProc.StatsResult {
        public long mutateMillis;

        public MutateResult(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long mutateMillis,
            long nodeCount,
            long triangleCount,
            double averageClusteringCoefficient,
            @Nullable Map<String, Object> communityDistribution,
            Map<String, Object> configuration
        ) {
            super(
                createMillis,
                computeMillis,
                postProcessingMillis,
                nodeCount,
                triangleCount,
                averageClusteringCoefficient,
                communityDistribution,
                configuration
            );

            this.mutateMillis = mutateMillis;
        }
    }

    static class TriangleCountMutateBuilder extends TriangleCountCompanion.TriangleCountResultBuilder<MutateResult> {

        TriangleCountMutateBuilder(
            ProcedureCallContext callContext,
            AllocationTracker tracker
        ) {
            super(callContext, tracker);
        }

        @Override
        protected MutateResult buildResult() {
            return new MutateResult(
                createMillis,
                computeMillis,
                postProcessingDuration,
                mutateMillis,
                nodeCount,
                triangleCount,
                averageClusteringCoefficient,
                communityHistogramOrNull(),
                config.toMap()
            );
        }
    }
}
