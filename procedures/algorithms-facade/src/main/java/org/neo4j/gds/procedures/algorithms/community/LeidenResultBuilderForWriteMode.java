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

import org.apache.commons.lang3.tuple.Pair;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTimings;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.leiden.LeidenResult;
import org.neo4j.gds.leiden.LeidenWriteConfig;
import org.neo4j.gds.result.CommunityStatistics;
import org.neo4j.gds.result.StatisticsComputationInstructions;

import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class LeidenResultBuilderForWriteMode implements ResultBuilder<LeidenWriteConfig, LeidenResult, Stream<LeidenWriteResult>, Pair<NodePropertiesWritten, NodePropertyValues>> {
    private final StatisticsComputationInstructions statisticsComputationInstructions;

    LeidenResultBuilderForWriteMode(StatisticsComputationInstructions statisticsComputationInstructions) {
        this.statisticsComputationInstructions = statisticsComputationInstructions;
    }

    @Override
    public Stream<LeidenWriteResult> build(
        Graph graph,
        LeidenWriteConfig configuration,
        Optional<LeidenResult> result,
        AlgorithmProcessingTimings timings,
        Optional<Pair<NodePropertiesWritten, NodePropertyValues>> metadata
    ) {
        if (result.isEmpty()) return Stream.of(LeidenWriteResult.emptyFrom(timings, configuration.toMap()));

        var leidenResult = result.get();

        var nodePropertiesWrittenAndConvertedNodePropertyValues = metadata.orElseThrow();

        var communityStatistics = CommunityStatistics.communityStats(
            nodePropertiesWrittenAndConvertedNodePropertyValues.getRight().nodeCount(),
            leidenResult.communities()::get,
            DefaultPool.INSTANCE,
            configuration.concurrency(),
            statisticsComputationInstructions
        );

        var communitySummary = CommunityStatistics.communitySummary(communityStatistics.histogram(), communityStatistics.success());

        var leidenWriteResult = new LeidenWriteResult(
            leidenResult.ranLevels(),
            leidenResult.didConverge(),
            leidenResult.communities().size(),
            communityStatistics.componentCount(),
            timings.preProcessingMillis,
            timings.computeMillis,
            communityStatistics.computeMilliseconds(),
            timings.mutateOrWriteMillis,
            nodePropertiesWrittenAndConvertedNodePropertyValues.getLeft().value(),
            communitySummary,
            Arrays.stream(leidenResult.modularities()).boxed().collect(Collectors.toList()),
            leidenResult.modularity(),
            configuration.toMap()
        );

        return Stream.of(leidenWriteResult);
    }
}
