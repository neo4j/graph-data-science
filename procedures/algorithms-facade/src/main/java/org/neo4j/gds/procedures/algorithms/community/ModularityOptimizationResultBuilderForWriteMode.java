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

import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationResult;
import org.neo4j.gds.modularityoptimization.ModularityOptimizationWriteConfig;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Optional;
import java.util.stream.Stream;

class ModularityOptimizationResultBuilderForWriteMode implements ResultBuilder<ModularityOptimizationWriteConfig, ModularityOptimizationResult, Stream<ModularityOptimizationWriteResult>, NodePropertiesWritten> {
    private final StatisticsComputationInstructions statisticsComputationInstructions;

    ModularityOptimizationResultBuilderForWriteMode(StatisticsComputationInstructions statisticsComputationInstructions) {
        this.statisticsComputationInstructions = statisticsComputationInstructions;
    }

    @Override
    public Stream<ModularityOptimizationWriteResult> build(
        Graph graph,
        GraphStore graphStore,
        ModularityOptimizationWriteConfig configuration,
        Optional<ModularityOptimizationResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return Stream.of(ModularityOptimizationWriteResult.emptyFrom(
            timings,
            configuration.toMap()
        ));

        var modularityOptimizationResult = result.get();

        var nodePropertyValues = CommunityCompanion.nodePropertyValues(
            configuration.isIncremental(),
            configuration.writeProperty(),
            configuration.seedProperty(),
            configuration.consecutiveIds(),
            modularityOptimizationResult.asNodeProperties(),
            configuration.minCommunitySize(),
            configuration.concurrency(),
            () -> graphStore.nodeProperty(configuration.seedProperty())
        );

        var communityStatistics = CommunityStatistics.communityStats(
            nodePropertyValues.nodeCount(),
            modularityOptimizationResult::communityId,
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            statisticsComputationInstructions
        );
        var componentCount = communityStatistics.componentCount();
        var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram(), communityStatistics.success());

        var modularityOptimizationWriteResult = new ModularityOptimizationWriteResult(
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatistics.computeMilliseconds(),
            timings.mutateOrWriteMillis,
            modularityOptimizationResult.asNodeProperties().nodeCount(),
            modularityOptimizationResult.didConverge(),
            modularityOptimizationResult.ranIterations(),
            modularityOptimizationResult.modularity(),
            componentCount,
            communitySummary,
            configuration.toMap()
        );

        return Stream.of(modularityOptimizationWriteResult);
    }
}
