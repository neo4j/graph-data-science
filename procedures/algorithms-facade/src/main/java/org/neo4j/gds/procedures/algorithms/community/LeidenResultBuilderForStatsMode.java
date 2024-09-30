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
package org.neo4j.gds.procedures.algorithms.community;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.leiden.LeidenStatsConfig;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class LeidenResultBuilderForStatsMode implements StatsResultBuilder<LeidenResult, Stream<LeidenStatsResult>> {
    private final CommunityStatisticsWithTimingComputer communityStatisticsWithTimingComputer = new CommunityStatisticsWithTimingComputer();

    private final LeidenStatsConfig configuration;
    private final StatisticsComputationInstructions statisticsComputationInstructions;

    LeidenResultBuilderForStatsMode(
        LeidenStatsConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        this.configuration = configuration;
        this.statisticsComputationInstructions = statisticsComputationInstructions;
    }

    @Override
    public Stream<LeidenStatsResult> build(
        Graph graph,
        Optional<LeidenResult> result,
        AlgorithmProcessingTimings timings
    ) {
        if (result.isEmpty()) return Stream.of(LeidenStatsResult.emptyFrom(timings, configuration.toMap()));

        var leidenResult = result.get();

        var communityStatisticsWithTiming = communityStatisticsWithTimingComputer.compute(
            configuration,
            statisticsComputationInstructions,
            graph.nodeCount(),
            leidenResult.communities()::get
        );

        var leidenStatsResult = new LeidenStatsResult(
            leidenResult.ranLevels(),
            leidenResult.didConverge(),
            leidenResult.communities().size(),
            communityStatisticsWithTiming.getLeft(),
            communityStatisticsWithTiming.getMiddle(),
            leidenResult.modularity(),
            Arrays.stream(leidenResult.modularities()).boxed().collect(Collectors.toList()),
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatisticsWithTiming.getRight(),
            configuration.toMap()
        );

        return Stream.of(leidenStatsResult);
    }
}
