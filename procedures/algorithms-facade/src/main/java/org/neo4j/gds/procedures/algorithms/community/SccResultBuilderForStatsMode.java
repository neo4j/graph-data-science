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
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.scc.SccStatsConfig;

import java.util.Optional;
import java.util.stream.Stream;

class SccResultBuilderForStatsMode implements StatsResultBuilder<HugeLongArray, Stream<SccStatsResult>> {
    private final CommunityStatisticsWithTimingComputer communityStatisticsWithTimingComputer = new CommunityStatisticsWithTimingComputer();

    private final SccStatsConfig configuration;
    private final StatisticsComputationInstructions statisticsComputationInstructions;

    SccResultBuilderForStatsMode(
        SccStatsConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {
        this.configuration = configuration;
        this.statisticsComputationInstructions = statisticsComputationInstructions;
    }

    @Override
    public Stream<SccStatsResult> build(
        Graph graph,
        Optional<HugeLongArray> result,
        AlgorithmProcessingTimings timings
    ) {
        if (result.isEmpty()) return Stream.of(SccStatsResult.emptyFrom(timings, configuration.toMap()));

        var hugeLongArray = result.get();

        var communityStatisticsWithTiming = communityStatisticsWithTimingComputer.compute(
            configuration,
            statisticsComputationInstructions,
            graph.nodeCount(),
            hugeLongArray::get
        );

        var sccStatsResult = new SccStatsResult(
            communityStatisticsWithTiming.getLeft(),
            communityStatisticsWithTiming.getMiddle(),
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatisticsWithTiming.getRight(),
            configuration.toMap()
        );

        return Stream.of(sccStatsResult);
    }
}
