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

import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.algorithms.community.CommunityCompanion;
import org.neo4j.gds.algorithms.community.specificfields.ApproxMaxKCutSpecificFields;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.approxmaxkcut.ApproxMaxKCutResult;
import org.neo4j.gds.approxmaxkcut.config.ApproxMaxKCutStreamConfig;
import org.neo4j.gds.procedures.community.approxmaxkcut.ApproxMaxKCutMutateResult;
import org.neo4j.gds.procedures.community.approxmaxkcut.ApproxMaxKCutStreamResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class ApproxMaxKCutComputationResultTransformer {

    private ApproxMaxKCutComputationResultTransformer() {}

    static Stream<ApproxMaxKCutStreamResult> toStreamResult(
        StreamComputationResult<ApproxMaxKCutResult> computationResult,
         ApproxMaxKCutStreamConfig config
    ) {
        return computationResult.result().map(approxMaxKCutResult -> {

            var graph = computationResult.graph();
            var nodeProperties = CommunityCompanion.nodePropertyValues(
                false,
                NodePropertyValuesAdapter.adapt(approxMaxKCutResult.candidateSolution()),
                config.minCommunitySize(),
                config.concurrency()
            );
            return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
                .filter(nodeProperties::hasValue)
                .mapToObj(nodeId -> new ApproxMaxKCutStreamResult(
                    graph.toOriginalNodeId(nodeId),
                    nodeProperties.longValue(nodeId)
                ));


        }).orElseGet(Stream::empty);
    }

    static ApproxMaxKCutMutateResult toMutateResult(NodePropertyMutateResult<ApproxMaxKCutSpecificFields> computationResult) {
        return new ApproxMaxKCutMutateResult(
            computationResult.nodePropertiesWritten(),
            computationResult.algorithmSpecificFields().cutCost(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.mutateMillis(),
            computationResult.configuration().toMap()
        );
    }
}
