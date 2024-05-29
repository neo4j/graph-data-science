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
package org.neo4j.gds.procedures.algorithms.centrality;

import org.neo4j.gds.api.IdMap;
import org.neo4j.gds.harmonic.HarmonicResult;

import java.util.Optional;
import java.util.stream.LongStream;
import java.util.stream.Stream;

/**
 * This is a duplicate of {@link org.neo4j.gds.procedures.algorithms.centrality.CentralityAlgorithmResultTransformer},
 * with slight modifications; leave it, it will wither and die one day
 */
class AlphaHarmonicResultTransformer {
    Stream<AlphaHarmonicStreamResult> transform(IdMap graph, Optional<HarmonicResult> result) {
        if (result.isEmpty()) return Stream.empty();

        var harmonicResult = result.get();
        var nodePropertyValues = harmonicResult.nodePropertyValues();

        return LongStream.range(IdMap.START_NODE_ID, graph.nodeCount())
            .filter(nodePropertyValues::hasValue)
            .mapToObj(nodeId -> new AlphaHarmonicStreamResult(
                graph.toOriginalNodeId(nodeId),
                nodePropertyValues.doubleValue(nodeId)
            ));
    }
}
