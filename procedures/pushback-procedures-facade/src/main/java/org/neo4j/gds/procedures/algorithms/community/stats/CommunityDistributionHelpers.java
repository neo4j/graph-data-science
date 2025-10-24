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

import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Map;
import java.util.function.LongUnaryOperator;

 class CommunityDistributionHelpers {

    private CommunityDistributionHelpers() {}

    static CommunityDistribution compute(
        NodePropertyValues nodePropertyValues,
        Concurrency concurrency,
        LongUnaryOperator communityFunction,
        StatisticsComputationInstructions statisticsComputationInstructions
    ) {

        var communityStatistics = CommunityStatistics.communityStats(
            nodePropertyValues.nodeCount(),
            communityFunction,
            DefaultPool.INSTANCE,
            concurrency,
            statisticsComputationInstructions
        );

        var communitySummary = CommunityStatistics.communitySummary(
            communityStatistics.histogram(),
            communityStatistics.success()
        );

        return new CommunityDistribution(communityStatistics,communitySummary);
    }

    record CommunityDistribution(CommunityStatistics.CommunityStats statistics, Map<String,Object> summary){

    }
}
