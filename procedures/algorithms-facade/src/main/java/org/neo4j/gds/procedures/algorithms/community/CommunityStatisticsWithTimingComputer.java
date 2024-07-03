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

import org.apache.commons.lang3.tuple.Triple;
import org.neo4j.gds.config.ConcurrencyConfig;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Map;
import java.util.function.LongUnaryOperator;

public class CommunityStatisticsWithTimingComputer {
    public Triple<Long, Map<String, Object>, Long> compute(
        ConcurrencyConfig configuration,
        StatisticsComputationInstructions statisticsComputationInstructions,
        long nodeCount,
        LongUnaryOperator communityFunction
    ) {
        var communityStatistics = CommunityStatistics.communityStats(
            nodeCount,
            communityFunction,
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            statisticsComputationInstructions
        );

        var componentCount = communityStatistics.componentCount();
        var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram());

        return Triple.of(componentCount, communitySummary, communityStatistics.computeMilliseconds());
    }
}
