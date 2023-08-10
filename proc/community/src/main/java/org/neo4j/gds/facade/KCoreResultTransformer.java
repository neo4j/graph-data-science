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
package org.neo4j.gds.facade;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.kcore.KCoreDecompositionBaseConfig;
import org.neo4j.gds.kcore.KCoreDecompositionResult;

import java.util.stream.LongStream;
import java.util.stream.Stream;

final class KCoreResultTransformer {

    private KCoreResultTransformer() {}

    static Stream<KCoreStreamResult> toStreamResult(ComputationResult<KCoreDecompositionBaseConfig, KCoreDecompositionResult> computationResult) {
        return computationResult.result().map(kCoreResult -> {
            var coreValues = kCoreResult.coreValues();
            var graph = computationResult.graph();
            return LongStream
                .range(IdMap.START_NODE_ID, graph.nodeCount())
                .mapToObj(nodeId ->
                    new KCoreStreamResult(
                        graph.toOriginalNodeId(nodeId),
                        coreValues.get(nodeId)
                    ));
        }).orElseGet(Stream::empty);
    }

}
