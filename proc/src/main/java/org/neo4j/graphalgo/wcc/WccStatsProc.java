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
package org.neo4j.graphalgo.wcc;

import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.StatsProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.graphalgo.results.MemoryEstimateResult;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.procedure.Mode.READ;

public class WccStatsProc extends StatsProc<Wcc, DisjointSetStruct, WccWriteProc.WriteResult, WccStreamConfig> {

    @Procedure(value = "gds.wcc.stats", mode = READ)
    @Description(STATS_DESCRIPTION)
    public Stream<StatsResult> stats(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        ComputationResult<Wcc, DisjointSetStruct, WccStreamConfig> computationResult = compute(
            graphNameOrConfig,
            configuration
        );
        return stats(computationResult).map(StatsResult::from);
    }

    @Procedure(value = "gds.wcc.stats.estimate", mode = READ)
    @Description(ESTIMATE_DESCRIPTION)
    public Stream<MemoryEstimateResult> statsEstimate(
        @Name(value = "graphName") Object graphNameOrConfig,
        @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
    ) {
        return computeEstimate(graphNameOrConfig, configuration);
    }

    @Override
    protected AbstractResultBuilder<WccWriteProc.WriteResult> resultBuilder(ComputationResult<Wcc, DisjointSetStruct, WccStreamConfig> computeResult) {
        WccWriteProc.WriteResult.WriteResultBuilder writeResultBuilder = new WccWriteProc.WriteResult.WriteResultBuilder(
            computeResult.graph().nodeCount(),
            callContext,
            computeResult.tracker()
        );
        return computeResult.result() != null
            ? writeResultBuilder.withCommunityFunction(computeResult.result()::setIdOf)
            : writeResultBuilder;
    }

    @Override
    protected WccStreamConfig newConfig(
        String username,
        Optional<String> graphName,
        Optional<GraphCreateConfig> maybeImplicitCreate,
        CypherMapWrapper config
    ) {
        return WccStreamConfig.of(username, graphName, maybeImplicitCreate, config);
    }

    @Override
    protected AlgorithmFactory<Wcc, WccStreamConfig> algorithmFactory(WccStreamConfig config) {
        return WccProc.algorithmFactory();
    }

    public static class StatsResult {

        public final long createMillis;
        public final long computeMillis;
        public final long postProcessingMillis;
        public final long componentCount;
        public final Map<String, Object> componentDistribution;
        public final Map<String, Object> configuration;

        StatsResult(
            long createMillis,
            long computeMillis,
            long postProcessingMillis,
            long componentCount,
            Map<String, Object> componentDistribution,
            Map<String, Object> configuration
        ) {

            this.createMillis = createMillis;
            this.computeMillis = computeMillis;
            this.postProcessingMillis = postProcessingMillis;
            this.componentCount = componentCount;
            this.componentDistribution = componentDistribution;
            this.configuration = configuration;
        }

        public static StatsResult from(WccWriteProc.WriteResult writeResult) {
            return new StatsResult(
                writeResult.createMillis,
                writeResult.computeMillis,
                writeResult.postProcessingMillis,
                writeResult.componentCount,
                writeResult.componentDistribution,
                writeResult.configuration
            );
        }
    }
}
