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
package org.neo4j.gds.procedures.algorithms.embeddings.stats;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.embeddings.fastrp.FastRPResult;
import org.neo4j.gds.procedures.algorithms.embeddings.FastRPStatsResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.results.ResultTransformer;

import java.util.Map;
import java.util.stream.Stream;

class FastRPStatsResultTransformer implements ResultTransformer<TimedAlgorithmResult<FastRPResult>, Stream<FastRPStatsResult>> {
    private final Graph graph;
    private final Map<String, Object> configuration;

    FastRPStatsResultTransformer(Graph graph, Map<String, Object> configuration) {
        this.graph = graph;
        this.configuration = configuration;
    }

    @Override
    public Stream<FastRPStatsResult> apply(TimedAlgorithmResult<FastRPResult> algorithmResult) {
        return Stream.of(new FastRPStatsResult(
            graph.nodeCount(),
            -1, // TODO
            algorithmResult.computeMillis(),
            configuration
        ));
    }
}
