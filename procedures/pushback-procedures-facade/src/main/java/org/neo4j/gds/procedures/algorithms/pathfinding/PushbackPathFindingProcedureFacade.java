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
import org.neo4j.gds.procedures.algorithms.pathfinding.mutate.PushbackPathFindingMutateProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.stats.PushbackPathFindingStatsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.stream.PushbackPathFindingStreamProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.PathFindingStubs;
import org.neo4j.gds.procedures.algorithms.pathfinding.write.PushbackPathFindingWriteProcedureFacade;
import org.neo4j.gds.procedures.algorithms.results.StandardModeResult;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;
import org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult;

import java.util.Map;
import java.util.stream.Stream;

public final class PushbackPathFindingProcedureFacade implements PathFindingProcedureFacade {

    private final PushbackPathFindingMutateProcedureFacade mutateProcedureFacade;
    private final PushbackPathFindingStatsProcedureFacade statsProcedureFacade;
    private final PushbackPathFindingStreamProcedureFacade streamProcedureFacade;
    private final PushbackPathFindingWriteProcedureFacade writeProcedureFacade;

    public PushbackPathFindingProcedureFacade(
        PushbackPathFindingMutateProcedureFacade mutateProcedureFacade,
        PushbackPathFindingStatsProcedureFacade statsProcedureFacade,
        PushbackPathFindingStreamProcedureFacade streamProcedureFacade,
        PushbackPathFindingWriteProcedureFacade writeProcedureFacade
    ) {
        this.mutateProcedureFacade = mutateProcedureFacade;
        this.statsProcedureFacade = statsProcedureFacade;
        this.streamProcedureFacade = streamProcedureFacade;
        this.writeProcedureFacade = writeProcedureFacade;
    }

    @Override
    public PathFindingStubs stubs() {
        return null;
    }

    @Override
    public Stream<AllShortestPathsStreamResult> allShortestPathStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.allShortestPaths(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> allShortestPathStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<BellmanFordStreamResult> bellmanFordStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.bellmanFord(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> bellmanFordStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<BellmanFordMutateResult> bellmanFordMutate(String graphName, Map<String, Object> configuration) {
        return mutateProcedureFacade.bellmanFord(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> bellmanFordMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<BellmanFordStatsResult> bellmanFordStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.bellmanFord(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> bellmanFordStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<BellmanFordWriteResult> bellmanFordWrite(String graphName, Map<String, Object> configuration) {
        return writeProcedureFacade.bellmanFord(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> bellmanFordWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingMutateResult> breadthFirstSearchMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return mutateProcedureFacade.breadthFirstSearch(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> breadthFirstSearchMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<StandardStatsResult> breadthFirstSearchStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsProcedureFacade.breadthFirstSearch(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> breadthFirstSearchStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<TraversalStreamResult> breadthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.breadthFirstSearch(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> breadthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingMutateResult> deltaSteppingMutate(String graphName, Map<String, Object> configuration) {
        return mutateProcedureFacade.deltaStepping(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> deltaSteppingMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<StandardStatsResult> deltaSteppingStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsProcedureFacade.deltaStepping(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> deltaSteppingStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingStreamResult> deltaSteppingStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.deltaStepping(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> deltaSteppingStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<StandardWriteRelationshipsResult> deltaSteppingWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return writeProcedureFacade.deltaStepping(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> deltaSteppingWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingMutateResult> depthFirstSearchMutate(String graphName, Map<String, Object> configuration) {
        return mutateProcedureFacade.depthFirstSearch(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> depthFirstSearchMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<TraversalStreamResult> depthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.depthFirstSearch(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> depthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<KSpanningTreeWriteResult> kSpanningTreeWrite(String graphName, Map<String, Object> configuration) {
        return writeProcedureFacade.kSpanningTree(graphName, configuration);
    }

    @Override
    public Stream<PathFindingStreamResult> longestPathStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.longestPath(graphName, configuration);
    }

    @Override
    public Stream<MaxFlowMutateResult> maxFlowMutate(String graphName, Map<String, Object> configuration) {
        return mutateProcedureFacade.maxFlow(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> maxFlowMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MaxFlowStreamResult> maxFlowStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.maxFlow(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> maxFlowStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MaxFlowStatsResult> maxFlowStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.maxFlow(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> maxFlowStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MaxFlowWriteResult> maxFlowWrite(String graphName, Map<String, Object> configuration) {
        return writeProcedureFacade.maxFlow(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> maxFlowWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpanningTreeStreamResult> prizeCollectingSteinerTreeStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.prizeCollectingSteinerTree(graphName, configuration);
    }

    @Override
    public Stream<PrizeCollectingSteinerTreeMutateResult> prizeCollectingSteinerTreeMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return mutateProcedureFacade.prizeCollectingSteinerTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> prizeCollectingSteinerTreeMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> prizeCollectingSteinerTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PrizeCollectingSteinerTreeStatsResult> prizeCollectingSteinerTreeStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsProcedureFacade.prizeCollectingSteinerTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> prizeCollectingSteinerTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PrizeCollectingSteinerTreeWriteResult> prizeCollectingSteinerTreeWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return writeProcedureFacade.pcst(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> prizeCollectingSteinerTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<StandardModeResult> randomWalkStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsProcedureFacade.randomWalk(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> randomWalkStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<RandomWalkStreamResult> randomWalkStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.randomWalk(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> randomWalkStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<RandomWalkMutateResult> randomWalkMutate(String graphName, Map<String, Object> configuration) {
        return mutateProcedureFacade.randomWalk(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> randomWalkMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingStreamResult> singlePairShortestPathAStarStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.singlePairShortestPathAStar(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathAStarStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingMutateResult> singlePairShortestPathAStarMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return mutateProcedureFacade.singlePairShortestPathAStar(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathAStarMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathAStarWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return writeProcedureFacade.singlePairShortestPathAStar(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathAStarWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingStreamResult> singlePairShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.singlePairShortestPathDijkstra(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingMutateResult> singlePairShortestPathDijkstraMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return mutateProcedureFacade.singlePairShortestPathDijkstra(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return writeProcedureFacade.singlePairShortestPathDijkstra(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingStreamResult> singlePairShortestPathYensStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.singlePairShortestPathYens(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathYensStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingMutateResult> singlePairShortestPathYensMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return mutateProcedureFacade.singlePairShortestPathYens(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathYensMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathYensWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return writeProcedureFacade.singlePairShortestPathYens(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathYensWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingStreamResult> singleSourceShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.singleSourceShortestPathDijkstra(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingMutateResult> singleSourceShortestPathDijkstraMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return mutateProcedureFacade.singleSourceShortestPathDijkstra(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<StandardWriteRelationshipsResult> singleSourceShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return writeProcedureFacade.singleSourceShortestPathDijkstra(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpanningTreeStatsResult> spanningTreeStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.spanningTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> spanningTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpanningTreeMutateResult> spanningTreeMutate(String graphName, Map<String, Object> configuration) {
        return mutateProcedureFacade.spanningTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> spanningTreeMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpanningTreeStreamResult> spanningTreeStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.spanningTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> spanningTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpanningTreeWriteResult> spanningTreeWrite(String graphName, Map<String, Object> configuration) {
        return writeProcedureFacade.spanningTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> spanningTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SteinerStatsResult> steinerTreeStats(String graphName, Map<String, Object> configuration) {
        return statsProcedureFacade.steinerTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> steinerTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SteinerMutateResult> steinerTreeMutate(String graphName, Map<String, Object> configuration) {
        return mutateProcedureFacade.steinerTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> steinerTreeMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SpanningTreeStreamResult> steinerTreeStream(String graphName, Map<String, Object> configuration) {
        return streamProcedureFacade.steinerTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> steinerTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<SteinerWriteResult> steinerTreeWrite(String graphName, Map<String, Object> configuration) {
        return writeProcedureFacade.steinerTree(graphName, configuration);
    }

    @Override
    public Stream<MemoryEstimateResult> steinerTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<TopologicalSortStreamResult> topologicalSortStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamProcedureFacade.topologicalSort(graphName, configuration);
    }
}
