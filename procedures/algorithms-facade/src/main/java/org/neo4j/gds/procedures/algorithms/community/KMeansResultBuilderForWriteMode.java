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
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.kmeans.KmeansResult;
import org.neo4j.gds.kmeans.KmeansWriteConfig;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Optional;
import java.util.stream.Stream;

class KMeansResultBuilderForWriteMode implements ResultBuilder<KmeansWriteConfig, KmeansResult, Stream<KmeansWriteResult>, NodePropertiesWritten> {
    private final StatisticsComputationInstructions statisticsComputationInstructions;
    private final boolean shouldComputeListOfCentroids;

    KMeansResultBuilderForWriteMode(
        StatisticsComputationInstructions statisticsComputationInstructions,
        boolean shouldComputeListOfCentroids
    ) {
        this.statisticsComputationInstructions = statisticsComputationInstructions;
        this.shouldComputeListOfCentroids = shouldComputeListOfCentroids;
    }

    @Override
    public Stream<KmeansWriteResult> build(
        Graph graph,
        GraphStore graphStore,
        KmeansWriteConfig configuration,
        Optional<KmeansResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return Stream.of(KmeansWriteResult.emptyFrom(timings, configuration.toMap()));

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

        var kmeansWriteResult = new KmeansWriteResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatistics.computeMilliseconds(),
            timings.mutateOrWriteMillis,
            metadata.orElseThrow().value,
            communitySummary,
            centroids,
            kmeansResult.averageDistanceToCentroid(),
            kmeansResult.averageSilhouette(),
            configuration.toMap()
        );

        return Stream.of(kmeansWriteResult);
    }
}
