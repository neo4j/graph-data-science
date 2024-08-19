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
import org.neo4j.gds.applications.algorithms.community.LouvainNodePropertyValuesComputer;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.louvain.LouvainMutateConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.procedures.algorithms.community.CommunityStatisticsWithTimingComputer;
import org.neo4j.gds.procedures.algorithms.community.LouvainMutateResult;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Optional;

public class LouvainResultBuilderForMutateMode implements ResultBuilder<LouvainMutateConfig, LouvainResult, LouvainMutateResult, NodePropertiesWritten> {
    private final CommunityStatisticsWithTimingComputer communityStatisticsWithTimingComputer = new CommunityStatisticsWithTimingComputer();

    private final StatisticsComputationInstructions statisticsComputationInstructions;

    public LouvainResultBuilderForMutateMode(StatisticsComputationInstructions statisticsComputationInstructions) {
        this.statisticsComputationInstructions = statisticsComputationInstructions;
    }

    @Override
    public LouvainMutateResult build(
        Graph graph,
        GraphStore graphStore,
        LouvainMutateConfig configuration,
        Optional<LouvainResult> result,
        AlgorithmProcessingTimings timings,
        Optional<NodePropertiesWritten> metadata
    ) {
        if (result.isEmpty()) return LouvainMutateResult.emptyFrom(timings, configuration.toMap());

        var louvainResult = result.get();

        var nodePropertyValues = new LouvainNodePropertyValuesComputer().compute(
            graphStore,
            configuration,
            configuration.mutateProperty(),
            louvainResult
        );

        var communityStatisticsWithTiming = communityStatisticsWithTimingComputer.compute(
            configuration,
            statisticsComputationInstructions,
            nodePropertyValues.nodeCount(),
            louvainResult::getCommunity
        );

        return LouvainMutateResult.create(
            louvainResult.modularity(),
            louvainResult.modularities(),
            louvainResult.ranLevels(),
            communityStatisticsWithTiming.getLeft(),
            communityStatisticsWithTiming.getMiddle(),
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatisticsWithTiming.getRight(),
            timings.mutateOrWriteMillis,
            metadata.orElseThrow().value(),
            configuration.toMap()
        );
    }
}
