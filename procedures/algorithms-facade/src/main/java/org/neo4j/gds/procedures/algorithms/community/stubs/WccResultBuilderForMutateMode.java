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
package org.neo4j.gds.procedures.algorithms.community.stubs;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.paged.dss.DisjointSetStruct;
import org.neo4j.gds.procedures.algorithms.community.WccMutateResult;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;
import org.neo4j.gds.wcc.WccMutateConfig;

import java.util.Optional;

public class WccResultBuilderForMutateMode implements ResultBuilder<WccMutateConfig, DisjointSetStruct, WccMutateResult, NodePropertiesWritten> {
    private final StatisticsComputationInstructions statisticsComputationInstructions;

    public WccResultBuilderForMutateMode(StatisticsComputationInstructions statisticsComputationInstructions) {
        this.statisticsComputationInstructions = statisticsComputationInstructions;
    }

    @Override
    public WccMutateResult build(
        Graph graph,
        GraphStore graphStore,
        WccMutateConfig configuration,
        Optional<DisjointSetStruct> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return WccMutateResult.emptyFrom(timings, configuration.toMap());

        var disjointSetStruct = result.get();

        var postProcessingStartTimeMillis = System.currentTimeMillis();

        var communityStatistics = CommunityStatistics.communityStats(
            disjointSetStruct.size(),
            disjointSetStruct::setIdOf,
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            statisticsComputationInstructions
        );
        var componentCount = communityStatistics.componentCount();
        var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram());

        var postProcessingEndTimeMillis = System.currentTimeMillis();
        var postProcessingDurationMillis = postProcessingEndTimeMillis - postProcessingStartTimeMillis;

        return new WccMutateResult(
            componentCount,
            communitySummary,
            timings.preProcessingMillis,
            timings.computeMillis,
            postProcessingDurationMillis,
            timings.mutateOrWriteMillis,
            metadata.orElseThrow().value,
            configuration.toMap()
        );
    }
}
