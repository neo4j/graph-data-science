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
package org.neo4j.gds.procedures.algorithms.community.stats;

import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.procedures.algorithms.community.CommunityDistributionHelpers;
import org.neo4j.gds.procedures.algorithms.community.LouvainStatsResult;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LouvainStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<LouvainResult>, Stream<LouvainStatsResult>> {

    private final Map<String, Object> configuration;
    private final StatisticsComputationInstructions statisticsComputationInstructions;
    private final Concurrency concurrency;

    public LouvainStatsResultTransformer(
        Map<String, Object> configuration,
        StatisticsComputationInstructions statisticsComputationInstructions,
        Concurrency concurrency
    ) {
        this.configuration = configuration;
        this.statisticsComputationInstructions = statisticsComputationInstructions;
        this.concurrency = concurrency;
    }

    @Override
    public Stream<LouvainStatsResult> apply(TimedAlgorithmResult<LouvainResult> timedAlgorithmResult) {

        var louvainResult = timedAlgorithmResult.result();
        var nodeCount = louvainResult.communities().size();
        var communities =  louvainResult.communities();

        var communityStatisticsWithTiming = CommunityDistributionHelpers.compute(
            nodeCount,
            concurrency,
            communities::get,
            statisticsComputationInstructions
        );

        var statistics = communityStatisticsWithTiming.statistics();

        var louvainStatsResult = new LouvainStatsResult(
            louvainResult.modularity(),
            Arrays.stream(louvainResult.modularities()).boxed().collect(Collectors.toList()),
            louvainResult.ranLevels(),
            statistics.componentCount(),
            communityStatisticsWithTiming.distribution(),
            0,
            timedAlgorithmResult.computeMillis(),
            statistics.computeMilliseconds(),
            configuration
        );

        return Stream.of(louvainStatsResult);

    }

}
