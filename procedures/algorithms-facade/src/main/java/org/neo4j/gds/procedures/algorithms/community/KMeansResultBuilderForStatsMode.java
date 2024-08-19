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
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.kmeans.KmeansStatsConfig;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Optional;
import java.util.stream.Stream;

class KMeansResultBuilderForStatsMode implements StatsResultBuilder<KmeansStatsConfig, KmeansResult, Stream<KmeansStatsResult>> {
    private final StatisticsComputationInstructions statisticsComputationInstructions;
    private final boolean shouldComputeListOfCentroids;

    KMeansResultBuilderForStatsMode(
        StatisticsComputationInstructions statisticsComputationInstructions,
        boolean shouldComputeListOfCentroids
    ) {
        this.statisticsComputationInstructions = statisticsComputationInstructions;
        this.shouldComputeListOfCentroids = shouldComputeListOfCentroids;
    }

    @Override
    public Stream<KmeansStatsResult> build(
        Graph graph,
        KmeansStatsConfig configuration,
        Optional<KmeansResult> result,
        AlgorithmProcessingTimings timings
    ) {
        if (result.isEmpty()) return Stream.of(KmeansStatsResult.emptyFrom(timings, configuration.toMap()));

        var kmeansResult = result.get();

        var nodePropertyValues = NodePropertyValuesAdapter.adapt(kmeansResult.communities());

        var communityStatistics = CommunityStatistics.communityStats(
            nodePropertyValues.nodeCount(),
            nodeId -> kmeansResult.communities().get(nodeId),
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            statisticsComputationInstructions
        );

        var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram(), communityStatistics.success());

        var centroids = new CentroidsComputer().compute(shouldComputeListOfCentroids, kmeansResult.centers());

        var kmeansStatsResult = new KmeansStatsResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatistics.computeMilliseconds(),
            communitySummary,
            centroids,
            kmeansResult.averageDistanceToCentroid(),
            kmeansResult.averageSilhouette(),
            configuration.toMap()
        );

        return Stream.of(kmeansStatsResult);
    }
}
