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

import org.neo4j.gds.algorithms.NodePropertyWriteResult;
import org.neo4j.gds.algorithms.StreamComputationResult;
import org.neo4j.gds.algorithms.community.specificfields.KCoreSpecificFields;
import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.kcore.KCoreDecompositionResult;
import org.neo4j.gds.procedures.community.kcore.KCoreDecompositionStreamResult;
import org.neo4j.gds.procedures.community.kcore.KCoreDecompositionWriteResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class KCoreComputationalResultTransformer {

    private KCoreComputationalResultTransformer() {}

    static Stream<KCoreDecompositionStreamResult> toStreamResult(StreamComputationResult<KCoreDecompositionResult> computationResult) {
        return computationResult.result().map(kCoreResult -> {
            var coreValues = kCoreResult.coreValues();
            var graph = computationResult.graph();
            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .mapToObj(nodeId ->
                    new KCoreDecompositionStreamResult(
                        graph.toOriginalNodeId(nodeId),
                        coreValues.get(nodeId)
                    ));
        }).orElseGet(Stream::empty);
    }

    static KCoreDecompositionWriteResult toWriteResult(NodePropertyWriteResult<KCoreSpecificFields> computationResult) {
        return new KCoreDecompositionWriteResult(
            computationResult.nodePropertiesWritten(),
            computationResult.algorithmSpecificFields().degeneracy(),
            computationResult.preProcessingMillis(),
            computationResult.computeMillis(),
            computationResult.postProcessingMillis(),
            computationResult.writeMillis(),
            computationResult.configuration().toMap()
        );
    }
}
