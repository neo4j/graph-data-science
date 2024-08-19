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
import org.neo4j.gds.applications.algorithms.community.LouvainNodePropertyValuesComputer;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.louvain.LouvainWriteConfig;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class LouvainResultBuilderForWriteMode implements ResultBuilder<LouvainWriteConfig, LouvainResult, Stream<LouvainWriteResult>, NodePropertiesWritten> {
    private final StatisticsComputationInstructions statisticsComputationInstructions;
    private final LouvainNodePropertyValuesComputer louvainNodePropertyValuesComputer = new LouvainNodePropertyValuesComputer();

    LouvainResultBuilderForWriteMode(StatisticsComputationInstructions statisticsComputationInstructions) {
        this.statisticsComputationInstructions = statisticsComputationInstructions;
    }

    @Override
    public Stream<LouvainWriteResult> build(
        Graph graph,
        GraphStore graphStore,
        LouvainWriteConfig configuration,
        Optional<LouvainResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return Stream.of(LouvainWriteResult.emptyFrom(timings, configuration.toMap()));

        var louvainResult = result.get();

        var nodePropertyValues = louvainNodePropertyValuesComputer.compute(
            graphStore,
            configuration,
            configuration.writeProperty(),
            louvainResult
        );

        var communityStatistics = CommunityStatistics.communityStats(
            nodePropertyValues.nodeCount(),
            louvainResult::getCommunity,
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            statisticsComputationInstructions
        );
        var componentCount = communityStatistics.componentCount();
        var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram(), communityStatistics.success());

        var leidenWriteResult = new LouvainWriteResult(
            louvainResult.modularity(),
            Arrays.stream(louvainResult.modularities()).boxed().collect(Collectors.toList()),
            louvainResult.ranLevels(),
            componentCount,
            communitySummary,
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatistics.computeMilliseconds(),
            timings.mutateOrWriteMillis,
            metadata.orElseThrow().value(),
            configuration.toMap()
        );

        return Stream.of(leidenWriteResult);
    }
}
