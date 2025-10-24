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
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.procedures.algorithms.community.LabelPropagationStatsResult;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

public class LabelPropagationStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<LabelPropagationResult>, Stream<LabelPropagationStatsResult>> {

    private final Map<String, Object> configuration;
    private final StatisticsComputationInstructions statisticsComputationInstructions;
    private final Concurrency concurrency;

    public LabelPropagationStatsResultTransformer(
        Map<String, Object> configuration,
        StatisticsComputationInstructions statisticsComputationInstructions,
        Concurrency concurrency
    ) {
        this.configuration = configuration;
        this.statisticsComputationInstructions = statisticsComputationInstructions;
        this.concurrency = concurrency;
    }

    @Override
    public Stream<LabelPropagationStatsResult> apply(TimedAlgorithmResult<LabelPropagationResult> timedAlgorithmResult) {

        var labelPropagationResult = timedAlgorithmResult.result();
        var nodeCount = labelPropagationResult.labels().size();
        var labels =  labelPropagationResult.labels();
        var communityStatisticsWithTiming = CommunityDistributionHelpers.compute(
            nodeCount,
            concurrency,
            labels::get,
            statisticsComputationInstructions
        );

        var statistics = communityStatisticsWithTiming.statistics();

        var labelPropagationStatsResult = new LabelPropagationStatsResult(
            labelPropagationResult.ranIterations(),
            labelPropagationResult.didConverge(),
            statistics.componentCount(),
            communityStatisticsWithTiming.summary(),
            0,
            timedAlgorithmResult.computeMillis(),
            statistics.computeMilliseconds(),
            configuration
        );

        return Stream.of(labelPropagationStatsResult);

    }

}
