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
package org.neo4j.gds.procedures.community;

import org.neo4j.gds.algorithms.LabelPropagationSpecificFields;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.StatsResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.algorithms.community.CommunityResultCompanion;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.labelpropagation.LabelPropagationBaseConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationResult;
import org.neo4j.gds.labelpropagation.LabelPropagationStatsConfig;
import org.neo4j.gds.labelpropagation.LabelPropagationStreamConfig;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationMutateResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationStatsResult;
import org.neo4j.gds.procedures.community.labelpropagation.LabelPropagationStreamResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class LabelPropagationComputationResultTransformer {

    private LabelPropagationComputationResultTransformer() {}

    static Stream<LabelPropagationStreamResult> toStreamResult(
        StreamComputationResult<LabelPropagationResult> computationResult,
        LabelPropagationStreamConfig configuration
    ) {
        return computationResult.result().map(result -> {
            var graph = computationResult.graph();


            var nodePropertyValues = CommunityResultCompanion.nodePropertyValues(
                false,
                configuration.consecutiveIds(),
                NodePropertyValuesAdapter.adapt(result.labels()),
                configuration.minCommunitySize(),
                configuration.concurrency()
            );


            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .filter(nodePropertyValues::hasValue)
                .mapToObj(nodeId -> new LabelPropagationStreamResult(
                    graph.toOriginalNodeId(nodeId),
                    nodePropertyValues.longValue(nodeId)
                ));
        }).orElseGet(Stream::empty);
    }

    static LabelPropagationMutateResult toMutateResult(
        NodePropertyMutateResult<LabelPropagationSpecificFields> computationResult,
        LabelPropagationBaseConfig mutateConfig
    ) {
        return new LabelPropagationMutateResult(
            computationResult.algorithmSpecificFields().ranIterations(),
            computationResult.algorithmSpecificFields().didConverge(),
            computationResult.algorithmSpecificFields().communityCount(),
            computationResult.algorithmSpecificFields().communityDistribution(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.mutateMillis(),
            computationResult.nodePropertiesWritten(),
            mutateConfig.toMap()
        );
    }

    static LabelPropagationStatsResult toStatsResult(
        StatsResult<LabelPropagationSpecificFields> computationResult,
        LabelPropagationStatsConfig statsConfig
    ) {
        return new LabelPropagationStatsResult(
            computationResult.algorithmSpecificFields().ranIterations(),
            computationResult.algorithmSpecificFields().didConverge(),
            computationResult.algorithmSpecificFields().communityCount(),
            computationResult.algorithmSpecificFields().communityDistribution(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            statsConfig.toMap()
        );
    }
}
