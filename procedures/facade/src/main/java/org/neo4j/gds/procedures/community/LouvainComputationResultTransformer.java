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

import org.neo4j.gds.CommunityProcCompanion;
import org.neo4j.gds.algorithms.LouvainSpecificFields;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.louvain.LouvainBaseConfig;
import org.neo4j.gds.louvain.LouvainResult;
import org.neo4j.gds.procedures.community.louvain.LouvainMutateResult;
import org.neo4j.gds.procedures.community.louvain.LouvainStreamResult;
import org.neo4j.gds.nodeproperties.LongNodePropertyValuesAdapter;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class LouvainComputationResultTransformer {
    private LouvainComputationResultTransformer() {}

    static Stream<LouvainStreamResult> toStreamResult(StreamComputationResult<LouvainBaseConfig, LouvainResult> computationResult) {
        return computationResult.result().map(louvainResult -> {
            var graph = computationResult.graph();
            var config=computationResult.configuration();

            var nodePropertyValues = CommunityProcCompanion.nodeProperties(
                config,
                LongNodePropertyValuesAdapter.create(louvainResult.dendrogramManager().getCurrent())
            );
            var includeIntermediateCommunities = config.includeIntermediateCommunities();

            return LongStream.range(0, graph.nodeCount())
                .boxed().
                filter(nodePropertyValues::hasValue)
                .map(nodeId -> {
                    var communities = includeIntermediateCommunities
                        ? louvainResult.getIntermediateCommunities(nodeId)
                        : null;
                    var communityId = nodePropertyValues.longValue(nodeId);
                    return LouvainStreamResult.create(graph.toOriginalNodeId(nodeId), communities, communityId);
                });
        }).orElseGet(Stream::empty);
    }

    static LouvainMutateResult toMutateResult(NodePropertyMutateResult<LouvainSpecificFields> computationResult) {
        return new LouvainMutateResult(
            computationResult.algorithmSpecificFields().modularity(),
            computationResult.algorithmSpecificFields().modularities(),
            computationResult.algorithmSpecificFields().ranLevels(),
            computationResult.algorithmSpecificFields().componentCount(),
            computationResult.algorithmSpecificFields().componentDistribution(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.mutateMillis(),
            computationResult.nodePropertiesWritten(),
            computationResult.configuration().toMap()
        );
    }


}
