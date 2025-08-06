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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackPathFindingStatsProcedureFacade {

    public Stream<BellmanFordStatsResult> bellmanFordStats(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    public Stream<StandardStatsResult> breadthFirstSearchStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    public Stream<StandardStatsResult> deltaSteppingStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    public Stream<PrizeCollectingSteinerTreeStatsResult> prizeCollectingSteinerTreeStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    public Stream<org.neo4j.gds.procedures.algorithms.results.StandardModeResult> randomWalkStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    public Stream<SpanningTreeStatsResult> spanningTreeStats(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    public Stream<SteinerStatsResult> steinerTreeStats(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

}
