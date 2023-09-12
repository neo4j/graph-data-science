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

import org.neo4j.gds.algorithms.K1ColoringSpecificFields;
import org.neo4j.gds.algorithms.NodePropertyMutateResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.algorithms.community.CommunityResultCompanion;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.api.properties.nodes.NodePropertyValuesAdapter;
import org.neo4j.gds.k1coloring.K1ColoringResult;
import org.neo4j.gds.k1coloring.K1ColoringStreamConfig;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringMutateResult;
import org.neo4j.gds.procedures.community.k1coloring.K1ColoringStreamResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class K1ColoringComputationResultTransformer {

    private K1ColoringComputationResultTransformer() {}

    static Stream<K1ColoringStreamResult> toStreamResult(
        StreamComputationResult<K1ColoringResult> computationResult,
        K1ColoringStreamConfig configuration
    ) {
        return computationResult.result().map(k1ColoringResult -> {
            var graph = computationResult.graph();
            var nodePropertyValues = CommunityResultCompanion.nodePropertyValues(
                false,
                false,
                NodePropertyValuesAdapter.adapt(k1ColoringResult.colors()),
                configuration.minCommunitySize(),
                configuration.concurrency()
            );
            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .filter(nodePropertyValues::hasValue)
                .mapToObj(nodeId -> new K1ColoringStreamResult(
                    graph.toOriginalNodeId(nodeId),
                    nodePropertyValues.longValue(nodeId)
                ));

        }).orElseGet(Stream::empty);
    }

    static K1ColoringMutateResult toMutateResult(NodePropertyMutateResult<K1ColoringSpecificFields> computationResult) {
        return new K1ColoringMutateResult(
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.mutateMillis(),
            computationResult.algorithmSpecificFields().nodeCount(),
            computationResult.algorithmSpecificFields().colorCount(),
            computationResult.algorithmSpecificFields().ranIterations(),
            computationResult.algorithmSpecificFields().didConverge(),
            computationResult.configuration().toMap()
        );
    }


}
