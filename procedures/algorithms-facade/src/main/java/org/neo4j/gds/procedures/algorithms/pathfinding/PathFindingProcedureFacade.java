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
import org.neo4j.gds.api.User;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.dag.longestPath.DagLongestPathStreamConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortStreamConfig;
import org.neo4j.gds.kspanningtree.KSpanningTreeWriteConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarStreamConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordStatsConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordStreamConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordWriteConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaStatsConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaStreamConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.traverse.BfsStatsConfig;
import org.neo4j.gds.paths.traverse.BfsStreamConfig;
import org.neo4j.gds.paths.traverse.DfsStreamConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.BellmanFordMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.BreadthFirstSearchMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.DeltaSteppingMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.DepthFirstSearchMutateStub;
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
import org.neo4j.gds.procedures.algorithms.stubs.GenericStub;
import org.neo4j.gds.spanningtree.SpanningTreeStatsConfig;
import org.neo4j.gds.spanningtree.SpanningTreeStreamConfig;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;
import org.neo4j.gds.steiner.SteinerTreeStatsConfig;
import org.neo4j.gds.steiner.SteinerTreeStreamConfig;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;
import org.neo4j.gds.traversal.RandomWalkStatsConfig;
import org.neo4j.gds.traversal.RandomWalkStreamConfig;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * This is the top facade on the Neo4j Procedures integration for path finding algorithms.
 * The role it plays is, to be newed up with request scoped dependencies,
 * and to capture the procedure-specific bits of path finding algorithms calls.
 * For example, translating a return column specification into a parameter, a business level concept.
 * This is also where we put result rendering.
 */
public final class PathFindingProcedureFacade {
    // request scoped services
    private final CloseableResourceRegistry closeableResourceRegistry;
    private final NodeLookup nodeLookup;
    private final ProcedureReturnColumns procedureReturnColumns;

    // delegate
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade;
    private final PathFindingAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade;
    private final PathFindingAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade;
    private final PathFindingAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade;

    // applications
    private final BellmanFordMutateStub bellmanFordMutateStub;
    private final BreadthFirstSearchMutateStub breadthFirstSearchMutateStub;
    private final DeltaSteppingMutateStub deltaSteppingMutateStub;
    private final DepthFirstSearchMutateStub depthFirstSearchMutateStub;
    private final RandomWalkMutateStub randomWalkMutateStub;
    private final SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub;
    private final SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub;
    private final SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub;
    private final SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub;
    private final SpanningTreeMutateStub spanningTreeMutateStub;
    private final SteinerTreeMutateStub steinerTreeMutateStub;

    // infrastructure
    private final ConfigurationParser configurationParser;
    private final User user;

    private PathFindingProcedureFacade(
        CloseableResourceRegistry closeableResourceRegistry,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationModeBusinessFacade,
        PathFindingAlgorithmsStatsModeBusinessFacade statsModeBusinessFacade,
        PathFindingAlgorithmsStreamModeBusinessFacade streamModeBusinessFacade,
        PathFindingAlgorithmsWriteModeBusinessFacade writeModeBusinessFacade,
        BellmanFordMutateStub bellmanFordMutateStub,
        BreadthFirstSearchMutateStub breadthFirstSearchMutateStub,
        DeltaSteppingMutateStub deltaSteppingMutateStub,
        DepthFirstSearchMutateStub depthFirstSearchMutateStub,
        RandomWalkMutateStub randomWalkMutateStub,
        SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub,
        SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub,
        SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub,
        SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub,
        SpanningTreeMutateStub spanningTreeMutateStub,
        SteinerTreeMutateStub steinerTreeMutateStub,
        ConfigurationParser configurationParser,
        User user
    ) {
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.nodeLookup = nodeLookup;
        this.procedureReturnColumns = procedureReturnColumns;

        this.estimationModeBusinessFacade = estimationModeBusinessFacade;
        this.statsModeBusinessFacade = statsModeBusinessFacade;
        this.streamModeBusinessFacade = streamModeBusinessFacade;
        this.writeModeBusinessFacade = writeModeBusinessFacade;

        this.bellmanFordMutateStub = bellmanFordMutateStub;
        this.breadthFirstSearchMutateStub = breadthFirstSearchMutateStub;
        this.deltaSteppingMutateStub = deltaSteppingMutateStub;
        this.depthFirstSearchMutateStub = depthFirstSearchMutateStub;
        this.randomWalkMutateStub = randomWalkMutateStub;
        this.singlePairShortestPathAStarMutateStub = singlePairShortestPathAStarMutateStub;
        this.singlePairShortestPathDijkstraMutateStub = singlePairShortestPathDijkstraMutateStub;
        this.singlePairShortestPathYensMutateStub = singlePairShortestPathYensMutateStub;
        this.singleSourceShortestPathDijkstraMutateStub = singleSourceShortestPathDijkstraMutateStub;
        this.spanningTreeMutateStub = spanningTreeMutateStub;
        this.steinerTreeMutateStub = steinerTreeMutateStub;

        this.configurationParser = configurationParser;
        this.user = user;
    }

    /**
     * Encapsulating some of the boring structure stuff
     */
    public static PathFindingProcedureFacade create(
        CloseableResourceRegistry closeableResourceRegistry,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns,
        ApplicationsFacade applicationsFacade,
        GenericStub genericStub,
        ConfigurationParser configurationParser,
        User user
    ) {
        var mutateModeBusinessFacade = applicationsFacade.pathFinding().mutate();
        var estimationModeBusinessFacade = applicationsFacade.pathFinding().estimate();
        var aStarStub = new SinglePairShortestPathAStarMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var bellmanFordMutateStub = new BellmanFordMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var breadthFirstSearchMutateStub = new BreadthFirstSearchMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var deltaSteppingMutateStub = new DeltaSteppingMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var depthFirstSearchMutateStub = new DepthFirstSearchMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var randomWalkMutateStub = new RandomWalkMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var singlePairDijkstraStub = new SinglePairShortestPathDijkstraMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var yensStub = new SinglePairShortestPathYensMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var singleSourceDijkstraStub = new SingleSourceShortestPathDijkstraMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var spanningTreeMutateStub = new SpanningTreeMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        var steinerTreeMutateStub = new SteinerTreeMutateStub(
            genericStub,
            mutateModeBusinessFacade,
            estimationModeBusinessFacade
        );

        return new PathFindingProcedureFacade(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns,
            estimationModeBusinessFacade,
            applicationsFacade.pathFinding().stats(),
            applicationsFacade.pathFinding().stream(),
            applicationsFacade.pathFinding().write(),
            bellmanFordMutateStub,
            breadthFirstSearchMutateStub,
            deltaSteppingMutateStub,
            depthFirstSearchMutateStub,
            randomWalkMutateStub,
            aStarStub,
            singlePairDijkstraStub,
            yensStub,
            singleSourceDijkstraStub,
            spanningTreeMutateStub,
            steinerTreeMutateStub,
            configurationParser,
            user
        );
    }

    public Stream<AllShortestPathsStreamResult> allShortestPathStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamModeBusinessFacade.allShortestPaths(
            GraphName.parse(graphName),
            parseConfiguration(configuration, AllShortestPathsConfig::of),
            (g, gs, c, result) -> result.orElse(Stream.empty())
        );
    }

    public BellmanFordMutateStub bellmanFordMutateStub() {
        return bellmanFordMutateStub;
    }

    public Stream<BellmanFordStreamResult> bellmanFordStream(String graphName, Map<String, Object> configuration) {
        var routeRequested = procedureReturnColumns.contains("route");
        var resultBuilder = new BellmanFordResultBuilderForStreamMode(
            closeableResourceRegistry,
            nodeLookup,
            routeRequested
        );

        return streamModeBusinessFacade.bellmanFord(
            GraphName.parse(graphName),
            parseConfiguration(configuration, AllShortestPathsBellmanFordStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> bellmanFordStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.bellmanFord(
                parseConfiguration(algorithmConfiguration, AllShortestPathsBellmanFordStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<BellmanFordStatsResult> bellmanFordStats(String graphName, Map<String, Object> configuration) {
        return statsModeBusinessFacade.bellmanFord(
            GraphName.parse(graphName),
            parseConfiguration(configuration, AllShortestPathsBellmanFordStatsConfig::of),
            new BellmanFordResultBuilderForStatsMode()
        );
    }

    public Stream<MemoryEstimateResult> bellmanFordStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.bellmanFord(
                parseConfiguration(algorithmConfiguration, AllShortestPathsBellmanFordStatsConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<BellmanFordWriteResult> bellmanFordWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            writeModeBusinessFacade.bellmanFord(
                GraphName.parse(graphName),
                parseConfiguration(configuration, AllShortestPathsBellmanFordWriteConfig::of),
                new BellmanFordResultBuilderForWriteMode()
            )
        );
    }

    public Stream<MemoryEstimateResult> bellmanFordWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.bellmanFord(
                parseConfiguration(algorithmConfiguration, AllShortestPathsBellmanFordWriteConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public BreadthFirstSearchMutateStub breadthFirstSearchMutateStub() {
        return breadthFirstSearchMutateStub;
    }

    public Stream<StandardStatsResult> breadthFirstSearchStats(String graphName, Map<String, Object> configuration) {
        return statsModeBusinessFacade.breadthFirstSearch(
            GraphName.parse(graphName),
            parseConfiguration(configuration, BfsStatsConfig::of),
            new BfsStatsResultBuilder()
        );
    }

    public Stream<MemoryEstimateResult> breadthFirstSearchStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.breadthFirstSearch(
                parseConfiguration(algorithmConfiguration, BfsStatsConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<BfsStreamResult> breadthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        return streamModeBusinessFacade.breadthFirstSearch(
            GraphName.parse(graphName),
            parseConfiguration(configuration, BfsStreamConfig::of),
            new BfsStreamResultBuilder(nodeLookup, procedureReturnColumns.contains("path"))
        );
    }

    public Stream<MemoryEstimateResult> breadthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.breadthFirstSearch(
                parseConfiguration(algorithmConfiguration, BfsStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public DeltaSteppingMutateStub deltaSteppingMutateStub() {
        return deltaSteppingMutateStub;
    }

    public Stream<StandardStatsResult> deltaSteppingStats(String graphName, Map<String, Object> configuration) {
        return statsModeBusinessFacade.deltaStepping(
            GraphName.parse(graphName),
            parseConfiguration(configuration, AllShortestPathsDeltaStatsConfig::of),
            new DeltaSteppingResultBuilderForStatsMode()
        );
    }

    public Stream<MemoryEstimateResult> deltaSteppingStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        return Stream.of(
            estimationModeBusinessFacade.deltaStepping(
                parseConfiguration(algorithmConfiguration, AllShortestPathsDeltaStatsConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<PathFindingStreamResult> deltaSteppingStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<AllShortestPathsDeltaStreamConfig>(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return streamModeBusinessFacade.deltaStepping(
            GraphName.parse(graphName),
            parseConfiguration(configuration, AllShortestPathsDeltaStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> deltaSteppingStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.deltaStepping(
                parseConfiguration(algorithmConfiguration, AllShortestPathsDeltaStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<StandardWriteRelationshipsResult> deltaSteppingWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            writeModeBusinessFacade.deltaStepping(
                GraphName.parse(graphName),
                parseConfiguration(configuration, AllShortestPathsDeltaWriteConfig::of),
                new PathFindingResultBuilderForWriteMode<>()
            )
        );
    }

    public Stream<MemoryEstimateResult> deltaSteppingWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.deltaStepping(
                parseConfiguration(algorithmConfiguration, AllShortestPathsDeltaWriteConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public DepthFirstSearchMutateStub depthFirstSearchMutateStub() {
        return depthFirstSearchMutateStub;
    }

    public Stream<DfsStreamResult> depthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        return streamModeBusinessFacade.depthFirstSearch(
            GraphName.parse(graphName),
            parseConfiguration(configuration, DfsStreamConfig::of),
            new DfsStreamResultBuilder(nodeLookup, procedureReturnColumns.contains("path"))
        );
    }

    public Stream<MemoryEstimateResult> depthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.depthFirstSearch(
                parseConfiguration(algorithmConfiguration, DfsStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<KSpanningTreeWriteResult> kSpanningTreeWrite(String graphName, Map<String, Object> configuration) {
        return Stream.of(
            writeModeBusinessFacade.kSpanningTree(
                GraphName.parse(graphName),
                parseConfiguration(configuration, KSpanningTreeWriteConfig::of),
                new KSpanningTreeResultBuilderForWriteMode()
            )
        );
    }

    public Stream<PathFindingStreamResult> longestPathStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<DagLongestPathStreamConfig>(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return streamModeBusinessFacade.longestPath(
            GraphName.parse(graphName),
            parseConfiguration(configuration, DagLongestPathStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<StandardModeResult> randomWalkStats(String graphName, Map<String, Object> configuration) {
        return statsModeBusinessFacade.randomWalk(
            GraphName.parse(graphName),
            parseConfiguration(configuration, RandomWalkStatsConfig::of),
            new RandomWalkResultBuilderForStatsMode()
        );
    }

    public Stream<MemoryEstimateResult> randomWalkStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.randomWalk(
                parseConfiguration(algorithmConfiguration, RandomWalkStatsConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<RandomWalkStreamResult> randomWalkStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new RandomWalkResultBuilderForStreamMode(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return streamModeBusinessFacade.randomWalk(
            GraphName.parse(graphName),
            parseConfiguration(configuration, RandomWalkStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> randomWalkStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        return Stream.of(
            estimationModeBusinessFacade.randomWalk(
                parseConfiguration(algorithmConfiguration, RandomWalkStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public RandomWalkMutateStub randomWalkMutateStub() {
        return randomWalkMutateStub;
    }

    public SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub() {
        return singlePairShortestPathAStarMutateStub;
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathAStarStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<ShortestPathAStarStreamConfig>(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return streamModeBusinessFacade.singlePairShortestPathAStar(
            GraphName.parse(graphName),
            parseConfiguration(configuration, ShortestPathAStarStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        return Stream.of(
            estimationModeBusinessFacade.singlePairShortestPathAStar(
                parseConfiguration(algorithmConfiguration, ShortestPathAStarStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathAStarWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            writeModeBusinessFacade.singlePairShortestPathAStar(
                GraphName.parse(graphName),
                parseConfiguration(configuration, ShortestPathAStarWriteConfig::of),
                new PathFindingResultBuilderForWriteMode<>()
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.singlePairShortestPathAStar(
                parseConfiguration(algorithmConfiguration, ShortestPathAStarWriteConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub() {
        return singlePairShortestPathDijkstraMutateStub;
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<ShortestPathDijkstraStreamConfig>(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return streamModeBusinessFacade.singlePairShortestPathDijkstra(
            GraphName.parse(graphName),
            parseConfiguration(configuration, ShortestPathDijkstraStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.singlePairShortestPathDijkstra(
                parseConfiguration(algorithmConfiguration, ShortestPathDijkstraStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            writeModeBusinessFacade.singlePairShortestPathDijkstra(
                GraphName.parse(graphName),
                parseConfiguration(configuration, ShortestPathDijkstraWriteConfig::of),
                new PathFindingResultBuilderForWriteMode<>()
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        return Stream.of(
            estimationModeBusinessFacade.singlePairShortestPathDijkstra(
                parseConfiguration(algorithmConfiguration, ShortestPathDijkstraWriteConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub() {
        return singlePairShortestPathYensMutateStub;
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathYensStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<ShortestPathYensStreamConfig>(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return streamModeBusinessFacade.singlePairShortestPathYens(
            GraphName.parse(graphName),
            parseConfiguration(configuration, ShortestPathYensStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {

        return Stream.of(
            estimationModeBusinessFacade.singlePairShortestPathYens(
                parseConfiguration(algorithmConfiguration, ShortestPathYensStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathYensWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            writeModeBusinessFacade.singlePairShortestPathYens(
                GraphName.parse(graphName),
                parseConfiguration(configuration, ShortestPathYensWriteConfig::of),
                new PathFindingResultBuilderForWriteMode<>()
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.singlePairShortestPathYens(
                parseConfiguration(algorithmConfiguration, ShortestPathYensWriteConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub() {
        return singleSourceShortestPathDijkstraMutateStub;
    }

    public Stream<PathFindingStreamResult> singleSourceShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<AllShortestPathsDijkstraStreamConfig>(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );
        return streamModeBusinessFacade.singleSourceShortestPathDijkstra(
            GraphName.parse(graphName),
            parseConfiguration(configuration, AllShortestPathsDijkstraStreamConfig::of),
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.singleSourceShortestPathDijkstra(
                parseConfiguration(algorithmConfiguration, AllShortestPathsDijkstraStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<StandardWriteRelationshipsResult> singleSourceShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            writeModeBusinessFacade.singleSourceShortestPathDijkstra(
                GraphName.parse(graphName),
                parseConfiguration(configuration, AllShortestPathsDijkstraWriteConfig::of),
                new PathFindingResultBuilderForWriteMode<>()
            )
        );
    }

    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.singleSourceShortestPathDijkstra(
                parseConfiguration(algorithmConfiguration, AllShortestPathsDijkstraWriteConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public SpanningTreeMutateStub spanningTreeMutateStub() {
        return spanningTreeMutateStub;
    }

    public Stream<SpanningTreeStatsResult> spanningTreeStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        return statsModeBusinessFacade.spanningTree(
            GraphName.parse(graphName),
            parseConfiguration(configuration, SpanningTreeStatsConfig::of),
            new SpanningTreeResultBuilderForStatsMode()
        );
    }

    public Stream<MemoryEstimateResult> spanningTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.spanningTree(
                parseConfiguration(algorithmConfiguration, SpanningTreeStatsConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<SpanningTreeStreamResult> spanningTreeStream(String graphName, Map<String, Object> configuration) {
        return streamModeBusinessFacade.spanningTree(
            GraphName.parse(graphName),
            parseConfiguration(configuration, SpanningTreeStreamConfig::of),
            new SpanningTreeResultBuilderForStreamMode()
        );
    }

    public Stream<MemoryEstimateResult> spanningTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.spanningTree(
                parseConfiguration(algorithmConfiguration, SpanningTreeStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<SpanningTreeWriteResult> spanningTreeWrite(String graphName, Map<String, Object> configuration) {
        return Stream.of(
            writeModeBusinessFacade.spanningTree(
                GraphName.parse(graphName),
                parseConfiguration(configuration, SpanningTreeWriteConfig::of),
                new SpanningTreeResultBuilderForWriteMode()
            )
        );
    }

    public Stream<MemoryEstimateResult> spanningTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.spanningTree(
                parseConfiguration(algorithmConfiguration, SpanningTreeWriteConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public SteinerTreeMutateStub steinerTreeMutateStub() {
        return steinerTreeMutateStub;
    }

    public Stream<SteinerStatsResult> steinerTreeStats(String graphName, Map<String, Object> configuration) {
        return statsModeBusinessFacade.steinerTree(
            GraphName.parse(graphName),
            parseConfiguration(configuration, SteinerTreeStatsConfig::of),
            new SteinerTreeResultBuilderForStatsMode()
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.steinerTree(
                parseConfiguration(algorithmConfiguration, SteinerTreeStatsConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<SteinerTreeStreamResult> steinerTreeStream(String graphName, Map<String, Object> configuration) {
        return streamModeBusinessFacade.steinerTree(
            GraphName.parse(graphName),
            parseConfiguration(configuration, SteinerTreeStreamConfig::of),
            new SteinerTreeResultBuilderForStreamMode()
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.steinerTree(
                parseConfiguration(algorithmConfiguration, SteinerTreeStreamConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<SteinerWriteResult> steinerTreeWrite(String graphName, Map<String, Object> configuration) {
        return Stream.of(
            writeModeBusinessFacade.steinerTree(
                GraphName.parse(graphName),
                parseConfiguration(configuration, SteinerTreeWriteConfig::of),
                new SteinerTreeResultBuilderForWriteMode()
            )
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        return Stream.of(
            estimationModeBusinessFacade.steinerTree(
                parseConfiguration(algorithmConfiguration, SteinerTreeWriteConfig::of),
                graphNameOrConfiguration
            )
        );
    }

    public Stream<TopologicalSortStreamResult> topologicalSortStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return streamModeBusinessFacade.topologicalSort(
            GraphName.parse(graphName),
            parseConfiguration(configuration, TopologicalSortStreamConfig::of),
            new TopologicalSortResultBuilderForStreamMode()
        );
    }

    private <C extends AlgoBaseConfig> C parseConfiguration(
        Map<String, Object> configuration,
        Function<CypherMapWrapper, C> configurationMapper
    ) {
        return configurationParser.parseConfiguration(
            configuration,
            configurationMapper,
            user
        );
    }
}
