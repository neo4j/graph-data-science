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

import org.neo4j.gds.allshortestpaths.AllShortestPathsConfig;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortStreamConfig;
import org.neo4j.gds.pathfinding.PathFindingComputeBusinessFacade;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordStreamConfig;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.PathFindingStubs;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;
import org.neo4j.gds.steiner.SteinerTreeStreamConfig;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class PushbackPathFindingProcedureFacade implements PathFindingProcedureFacade {

    private final PathFindingComputeBusinessFacade businessFacade;

    private final UserSpecificConfigurationParser configurationParser;

    private final CloseableResourceRegistry closeableResourceRegistry;
    private final NodeLookup nodeLookup;
    private final ProcedureReturnColumns procedureReturnColumns;

    public PushbackPathFindingProcedureFacade(
        PathFindingComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        CloseableResourceRegistry closeableResourceRegistry,
        NodeLookup nodeLookup, ProcedureReturnColumns procedureReturnColumns
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.nodeLookup = nodeLookup;
        this.procedureReturnColumns = procedureReturnColumns;
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
        var config = configurationParser.parseConfiguration(configuration, AllShortestPathsConfig::of);

        return businessFacade.allShortestPaths(
                GraphName.parse(graphName),
                config.toGraphParameters(),
                config.relationshipWeightProperty(),
                config.toParameters(),
                config.jobId(),
                (graph, graphStore) -> r -> r // `MSBFSASPAlgorithm` implementations already maps the ids to the original ones => no need for transformation
            )
            .join();
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
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsBellmanFordStreamConfig::of
        );

        var routeRequested = procedureReturnColumns.contains("route");
        var resultTransformerBuilder = new BellmanFordStreamResultTransformerBuilder(
            closeableResourceRegistry,
            nodeLookup,
            routeRequested
        );

        return businessFacade.bellmanFord(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            resultTransformerBuilder
        ).join();
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
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> breadthFirstSearchStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<BfsStreamResult> breadthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> deltaSteppingStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult> deltaSteppingWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
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
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> depthFirstSearchMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<DfsStreamResult> depthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
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
        return Stream.empty();
    }

    @Override
    public Stream<PathFindingStreamResult> longestPathStream(String graphName, Map<String, Object> configuration) {
        return Stream.empty();
    }

    @Override
    public Stream<SpanningTreeStreamResult> prizeCollectingSteinerTreeStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<PrizeCollectingSteinerTreeMutateResult> prizeCollectingSteinerTreeMutate(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> prizeCollectingSteinerTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<org.neo4j.gds.procedures.algorithms.results.StandardModeResult> randomWalkStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathAStarMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult> singlePairShortestPathAStarWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult> singlePairShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> singlePairShortestPathYensMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult> singlePairShortestPathYensWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
    }

    @Override
    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraMutateEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.empty();
    }

    @Override
    public Stream<org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult> singleSourceShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
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
        return  Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
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
        return Stream.empty();
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
        var config = configurationParser.parseConfiguration(
            configuration,
            SteinerTreeStreamConfig::of
        );
        var resultTransformerBuilder  = new SteinerTreeStreamResultTransformerBuilder(config.sourceNode());
        return businessFacade.steinerTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            resultTransformerBuilder
        ).join();
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
        return Stream.empty();
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

        var config = configurationParser.parseConfiguration(
            configuration,
            TopologicalSortStreamConfig::of
        );

        return businessFacade.topologicalSort(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            Optional.empty(), //no exposed property?
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new TopologicalSortStreamResultTransformerBuilder()
        ).join();
    }
}
