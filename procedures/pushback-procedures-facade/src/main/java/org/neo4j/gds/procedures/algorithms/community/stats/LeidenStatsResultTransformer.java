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
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.procedures.algorithms.community.LeidenStatsResult;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LeidenStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<LeidenResult>, Stream<LeidenStatsResult>> {

    private final Map<String, Object> configuration;
    private final StatisticsComputationInstructions statisticsComputationInstructions;
    private final Concurrency concurrency;

    public LeidenStatsResultTransformer(
        Map<String, Object> configuration,
        StatisticsComputationInstructions statisticsComputationInstructions,
        Concurrency concurrency
    ) {
        this.configuration = configuration;
        this.statisticsComputationInstructions = statisticsComputationInstructions;
        this.concurrency = concurrency;
    }

    @Override
    public Stream<LeidenStatsResult> apply(TimedAlgorithmResult<LeidenResult> timedAlgorithmResult) {

        var leidenResult = timedAlgorithmResult.result();
        var nodeCount = leidenResult.communities().size();
        var communities =  leidenResult.communities();

        var communityStatisticsWithTiming = CommunityDistributionHelpers.compute(
            nodeCount,
            concurrency,
            communities::get,
            statisticsComputationInstructions
        );

        var statistics = communityStatisticsWithTiming.statistics();

        var leidenStatsResult = new LeidenStatsResult(
            leidenResult.ranLevels(),
            leidenResult.didConverge(),
            leidenResult.communities().size(),
            statistics.componentCount(),
            communityStatisticsWithTiming.distribution(),
            leidenResult.modularity(),
            Arrays.stream(leidenResult.modularities()).boxed().collect(Collectors.toList()),
            0,
            timedAlgorithmResult.computeMillis(),
            statistics.computeMilliseconds(),
            configuration
        );

        return Stream.of(leidenStatsResult);

    }

}
