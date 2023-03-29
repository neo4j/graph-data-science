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
package org.neo4j.gds.triangle;

import org.neo4j.gds.GraphAlgorithmFactory;
import org.neo4j.gds.StatsProc;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.GdsCallable;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.stream.Stream;

import static org.neo4j.gds.AlgoBaseProc.STATS_DESCRIPTION;
import static org.neo4j.gds.executor.ExecutionMode.STATS;
import static org.neo4j.procedure.Mode.READ;

@GdsCallable(name = "gds.triangleCount.stats", description = STATS_DESCRIPTION, executionMode = STATS)
public class TriangleCountStatsProc extends StatsProc<IntersectingTriangleCount, TriangleCountResult, TriangleCountStatsProc.StatsResult, TriangleCountStatsConfig> {

    @Procedure(value = "gds.triangleCount.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") String graphName,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return stats(compute(graphName, configuration));
    }

    @Procedure(value = "gds.triangleCount.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> estimate(
        @Name(value = "graphNameOrConfiguration") Object graphNameOrConfiguration,
        @Name(value = "algoConfiguration") Map<String, Object> algoConfiguration
    ) {
        return computeEstimate(graphNameOrConfiguration, algoConfiguration);
    }

    @Override
    protected AbstractResultBuilder<StatsResult> resultBuilder(
        ComputationResult<IntersectingTriangleCount, TriangleCountResult, TriangleCountStatsConfig> computeResult,
        ExecutionContext executionContext
    ) {
        return TriangleCountCompanion.resultBuilder(new TriangleCountStatsBuilder(), computeResult);
    }

    @Override
    protected TriangleCountStatsConfig newConfig(String username, CypherMapWrapper config) {
        return TriangleCountStatsConfig.of(config);
    }

    @Override
    public GraphAlgorithmFactory<IntersectingTriangleCount, TriangleCountStatsConfig> algorithmFactory() {
        return new IntersectingTriangleCountFactory<>();
    }

    @SuppressWarnings("unused")
    public static class StatsResult extends StandardStatsResult {

        public final long globalTriangleCount;
        public final long nodeCount;

        StatsResult(
            long globalTriangleCount,
            long nodeCount,
            long preProcessingMillis,
            long computeMillis,
            Map<String, Object> configuration
        ) {
            // post-processing is instant for TC
            super(preProcessingMillis, computeMillis, 0L, configuration);
            this.globalTriangleCount = globalTriangleCount;
            this.nodeCount = nodeCount;
        }
    }

    static class TriangleCountStatsBuilder extends TriangleCountCompanion.TriangleCountResultBuilder<StatsResult> {

        @Override
        public StatsResult build() {
            return new StatsResult(
                globalTriangleCount,
                nodeCount,
                preProcessingMillis,
                computeMillis,
                config.toMap()
            );
        }
    }
}
