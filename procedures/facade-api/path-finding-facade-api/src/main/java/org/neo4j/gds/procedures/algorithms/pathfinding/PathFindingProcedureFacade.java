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

import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.BFSMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.BellmanFordMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.DFSMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.DeltaSteppingMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.RandomWalkMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SinglePairShortestPathAStarMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SinglePairShortestPathDijkstraMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SinglePairShortestPathYensMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SingleSourceShortestPathDijkstraMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SpanningTreeMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SteinerTreeMutateStub;
import org.neo4j.gds.procedures.algorithms.results.StandardModeResult;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;
import org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult;

import java.util.Map;
import java.util.stream.Stream;

public interface PathFindingProcedureFacade {
    Stream<AllShortestPathsStreamResult> allShortestPathStream(
        String graphName,
        Map<String, Object> configuration
    );

    BellmanFordMutateStub bellmanFordMutateStub();

    Stream<BellmanFordStreamResult> bellmanFordStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> bellmanFordStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<BellmanFordStatsResult> bellmanFordStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> bellmanFordStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<BellmanFordWriteResult> bellmanFordWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> bellmanFordWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    BFSMutateStub breadthFirstSearchMutateStub();

    Stream<StandardStatsResult> breadthFirstSearchStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> breadthFirstSearchStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<BfsStreamResult> breadthFirstSearchStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> breadthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    DeltaSteppingMutateStub deltaSteppingMutateStub();

    Stream<StandardStatsResult> deltaSteppingStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> deltaSteppingStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<PathFindingStreamResult> deltaSteppingStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> deltaSteppingStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<StandardWriteRelationshipsResult> deltaSteppingWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> deltaSteppingWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    DFSMutateStub depthFirstSearchMutateStub();

    Stream<DfsStreamResult> depthFirstSearchStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> depthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<KSpanningTreeWriteResult> kSpanningTreeWrite(String graphName, Map<String, Object> configuration);

    Stream<PathFindingStreamResult> longestPathStream(String graphName, Map<String, Object> configuration);

    Stream<StandardModeResult> randomWalkStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> randomWalkStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<RandomWalkStreamResult> randomWalkStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> randomWalkStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    RandomWalkMutateStub randomWalkMutateStub();

    SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub();

    Stream<PathFindingStreamResult> singlePairShortestPathAStarStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> singlePairShortestPathAStarStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<StandardWriteRelationshipsResult> singlePairShortestPathAStarWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> singlePairShortestPathAStarWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub();

    Stream<PathFindingStreamResult> singlePairShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> singlePairShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<StandardWriteRelationshipsResult> singlePairShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> singlePairShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub();

    Stream<PathFindingStreamResult> singlePairShortestPathYensStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> singlePairShortestPathYensStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<StandardWriteRelationshipsResult> singlePairShortestPathYensWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> singlePairShortestPathYensWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub();

    Stream<PathFindingStreamResult> singleSourceShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<StandardWriteRelationshipsResult> singleSourceShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    SpanningTreeMutateStub spanningTreeMutateStub();

    Stream<SpanningTreeStatsResult> spanningTreeStats(
        String graphName,
        Map<String, Object> configuration
    );

    Stream<MemoryEstimateResult> spanningTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SpanningTreeStreamResult> spanningTreeStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> spanningTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SpanningTreeWriteResult> spanningTreeWrite(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> spanningTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    SteinerTreeMutateStub steinerTreeMutateStub();

    Stream<SteinerStatsResult> steinerTreeStats(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> steinerTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SteinerTreeStreamResult> steinerTreeStream(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> steinerTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<SteinerWriteResult> steinerTreeWrite(String graphName, Map<String, Object> configuration);

    Stream<MemoryEstimateResult> steinerTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    );

    Stream<TopologicalSortStreamResult> topologicalSortStream(
        String graphName,
        Map<String, Object> configuration
    );
}
