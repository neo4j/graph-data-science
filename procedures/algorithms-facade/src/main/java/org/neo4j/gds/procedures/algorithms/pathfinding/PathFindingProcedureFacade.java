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
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsEstimationModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsMutateModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsStatsModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsStreamModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.PathFindingAlgorithmsWriteModeBusinessFacade;
import org.neo4j.gds.applications.algorithms.pathfinding.ResultBuilder;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.configuration.DefaultsConfiguration;
import org.neo4j.gds.configuration.LimitsConfiguration;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.dag.longestPath.DagLongestPathStreamConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortStreamConfig;
import org.neo4j.gds.kspanningtree.KSpanningTreeWriteConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarStreamConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordStatsConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordStreamConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordWriteConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaStatsConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaStreamConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.traverse.BfsStatsConfig;
import org.neo4j.gds.paths.traverse.BfsStreamConfig;
import org.neo4j.gds.paths.traverse.DfsStreamConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationCreator;
import org.neo4j.gds.procedures.algorithms.configuration.ConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.BreadthFirstSearchMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.DeltaSteppingMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.DepthFirstSearchMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.GenericStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SinglePairShortestPathAStarMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SinglePairShortestPathDijkstraMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SinglePairShortestPathYensMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SingleSourceShortestPathDijkstraMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SpanningTreeMutateStub;
import org.neo4j.gds.procedures.algorithms.pathfinding.stubs.SteinerTreeMutateStub;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.results.StandardModeResult;
import org.neo4j.gds.results.StandardStatsResult;
import org.neo4j.gds.results.StandardWriteRelationshipsResult;
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
    private final ConfigurationCreator configurationCreator;
    private final NodeLookup nodeLookup;
    private final ProcedureReturnColumns procedureReturnColumns;

    // delegate
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationModeFacade;
    private final PathFindingAlgorithmsStatsModeBusinessFacade statsModeFacade;
    private final PathFindingAlgorithmsStreamModeBusinessFacade streamModeFacade;
    private final PathFindingAlgorithmsWriteModeBusinessFacade writeModeFacade;

    // applications
    private final BreadthFirstSearchMutateStub breadthFirstSearchMutateStub;
    private final DeltaSteppingMutateStub deltaSteppingMutateStub;
    private final DepthFirstSearchMutateStub depthFirstSearchMutateStub;
    private final SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub;
    private final SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub;
    private final SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub;
    private final SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub;
    private final SpanningTreeMutateStub spanningTreeMutateStub;
    private final SteinerTreeMutateStub steinerTreeMutateStub;

    private PathFindingProcedureFacade(
        CloseableResourceRegistry closeableResourceRegistry,
        ConfigurationCreator configurationCreator,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationModeFacade,
        PathFindingAlgorithmsStatsModeBusinessFacade statsModeFacade,
        PathFindingAlgorithmsStreamModeBusinessFacade streamModeFacade,
        PathFindingAlgorithmsWriteModeBusinessFacade writeModeFacade,
        BreadthFirstSearchMutateStub breadthFirstSearchMutateStub,
        DeltaSteppingMutateStub deltaSteppingMutateStub,
        DepthFirstSearchMutateStub depthFirstSearchMutateStub,
        SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub,
        SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub,
        SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub,
        SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub,
        SpanningTreeMutateStub spanningTreeMutateStub,
        SteinerTreeMutateStub steinerTreeMutateStub
    ) {
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.configurationCreator = configurationCreator;
        this.nodeLookup = nodeLookup;
        this.procedureReturnColumns = procedureReturnColumns;

        this.estimationModeFacade = estimationModeFacade;
        this.statsModeFacade = statsModeFacade;
        this.streamModeFacade = streamModeFacade;
        this.writeModeFacade = writeModeFacade;

        this.breadthFirstSearchMutateStub = breadthFirstSearchMutateStub;
        this.deltaSteppingMutateStub = deltaSteppingMutateStub;
        this.depthFirstSearchMutateStub = depthFirstSearchMutateStub;
        this.singlePairShortestPathAStarMutateStub = singlePairShortestPathAStarMutateStub;
        this.singlePairShortestPathDijkstraMutateStub = singlePairShortestPathDijkstraMutateStub;
        this.singlePairShortestPathYensMutateStub = singlePairShortestPathYensMutateStub;
        this.singleSourceShortestPathDijkstraMutateStub = singleSourceShortestPathDijkstraMutateStub;
        this.spanningTreeMutateStub = spanningTreeMutateStub;
        this.steinerTreeMutateStub = steinerTreeMutateStub;
    }

    /**
     * Encapsulating some of the boring structure stuff
     */
    public static PathFindingProcedureFacade create(
        DefaultsConfiguration defaultsConfiguration,
        LimitsConfiguration limitsConfiguration,
        CloseableResourceRegistry closeableResourceRegistry,
        ConfigurationCreator configurationCreator,
        ConfigurationParser configurationParser,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns,
        User user,
        PathFindingAlgorithmsEstimationModeBusinessFacade pathFindingAlgorithmsEstimationModeBusinessFacade,
        PathFindingAlgorithmsMutateModeBusinessFacade pathFindingAlgorithmsMutateModeBusinessFacade,
        PathFindingAlgorithmsStatsModeBusinessFacade pathFindingAlgorithmsStatsModeBusinessFacade,
        PathFindingAlgorithmsStreamModeBusinessFacade pathFindingAlgorithmsStreamModeBusinessFacade,
        PathFindingAlgorithmsWriteModeBusinessFacade pathFindingAlgorithmsWriteModeBusinessFacade
    ) {
        var genericStub = new GenericStub(
            defaultsConfiguration,
            limitsConfiguration,
            configurationCreator,
            configurationParser,
            user,
            pathFindingAlgorithmsEstimationModeBusinessFacade
        );

        var aStarStub = new SinglePairShortestPathAStarMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var breadthFirstSearchMutateStub = new BreadthFirstSearchMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var deltaSteppingMutateStub = new DeltaSteppingMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var depthFirstSearchMutateStub = new DepthFirstSearchMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var singlePairDijkstraStub = new SinglePairShortestPathDijkstraMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var yensStub = new SinglePairShortestPathYensMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var singleSourceDijkstraStub = new SingleSourceShortestPathDijkstraMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var spanningTreeMutateStub = new SpanningTreeMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        var steinerTreeMutateStub = new SteinerTreeMutateStub(
            genericStub,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsMutateModeBusinessFacade
        );

        return new PathFindingProcedureFacade(
            closeableResourceRegistry,
            configurationCreator,
            nodeLookup,
            procedureReturnColumns,
            pathFindingAlgorithmsEstimationModeBusinessFacade,
            pathFindingAlgorithmsStatsModeBusinessFacade,
            pathFindingAlgorithmsStreamModeBusinessFacade,
            pathFindingAlgorithmsWriteModeBusinessFacade,
            breadthFirstSearchMutateStub,
            deltaSteppingMutateStub,
            depthFirstSearchMutateStub,
            aStarStub,
            singlePairDijkstraStub,
            yensStub,
            singleSourceDijkstraStub,
            spanningTreeMutateStub,
            steinerTreeMutateStub
        );
    }

    public Stream<AllShortestPathsStreamResult> allShortestPathStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        ResultBuilder<AllShortestPathsConfig, Stream<AllShortestPathsStreamResult>, Stream<AllShortestPathsStreamResult>> resultBuilder =
            (__, ___, ____, result, _____, ______) -> result.orElse(Stream.empty());

        return runStreamAlgorithm(
            graphName,
            configuration,
            AllShortestPathsConfig::of,
            resultBuilder,
            streamModeFacade::allShortestPaths
        );
    }

    public Stream<BellmanFordStreamResult> bellmanFordStream(String graphName, Map<String, Object> configuration) {
        var routeRequested = procedureReturnColumns.contains("route");
        var resultBuilder = new BellmanFordResultBuilderForStreamMode(nodeLookup, routeRequested);

        return runStreamAlgorithm(
            graphName,
            configuration,
            BellmanFordStreamConfig::of,
            resultBuilder,
            streamModeFacade::bellmanFord
        );
    }

    public Stream<MemoryEstimateResult> bellmanFordStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            BellmanFordStreamConfig::of,
            configuration -> estimationModeFacade.bellmanFord(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<BellmanFordStatsResult> bellmanFordStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new BellmanFordResultBuilderForStatsMode();

        return runStatsAlgorithm(
            graphName,
            configuration,
            BellmanFordStatsConfig::of,
            resultBuilder,
            statsModeFacade::bellmanFord
        );
    }

    public Stream<MemoryEstimateResult> bellmanFordStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            BellmanFordStatsConfig::of,
            configuration -> estimationModeFacade.bellmanFord(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<BellmanFordWriteResult> bellmanFordWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new BellmanFordResultBuilderForWriteMode();

        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                BellmanFordWriteConfig::of,
                writeModeFacade::bellmanFord,
                resultBuilder
            )
        );
    }

    public Stream<MemoryEstimateResult> bellmanFordWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            BellmanFordWriteConfig::of,
            configuration -> estimationModeFacade.bellmanFord(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public BreadthFirstSearchMutateStub breadthFirstSearchMutateStub() {
        return breadthFirstSearchMutateStub;
    }

    public Stream<StandardStatsResult> breadthFirstSearchStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new BfsStatsResultBuilder();

        return runStatsAlgorithm(
            graphName,
            configuration,
            BfsStatsConfig::of,
            resultBuilder,
            statsModeFacade::breadthFirstSearch
        );
    }

    public Stream<MemoryEstimateResult> breadthFirstSearchStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            BfsStatsConfig::of,
            configuration -> estimationModeFacade.breadthFirstSearch(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<BfsStreamResult> breadthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new BfsStreamResultBuilder(nodeLookup, procedureReturnColumns.contains("path"));

        return runStreamAlgorithm(
            graphName,
            configuration,
            BfsStreamConfig::of,
            resultBuilder,
            streamModeFacade::breadthFirstSearch
        );
    }

    public Stream<MemoryEstimateResult> breadthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            BfsStreamConfig::of,
            configuration -> estimationModeFacade.breadthFirstSearch(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public DeltaSteppingMutateStub deltaSteppingMutateStub() {
        return deltaSteppingMutateStub;
    }

    public Stream<StandardStatsResult> deltaSteppingStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new DeltaSteppingResultBuilderForStatsMode();

        return runStatsAlgorithm(
            graphName,
            configuration,
            AllShortestPathsDeltaStatsConfig::of,
            resultBuilder,
            statsModeFacade::deltaStepping
        );
    }

    public Stream<MemoryEstimateResult> deltaSteppingStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            AllShortestPathsDeltaStatsConfig::of,
            configuration -> estimationModeFacade.deltaStepping(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<PathFindingStreamResult> deltaSteppingStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<AllShortestPathsDeltaStreamConfig>(
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return runStreamAlgorithm(
            graphName,
            configuration,
            AllShortestPathsDeltaStreamConfig::of,
            resultBuilder,
            streamModeFacade::deltaStepping
        );
    }

    public Stream<MemoryEstimateResult> deltaSteppingStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            AllShortestPathsDeltaStreamConfig::of,
            configuration -> estimationModeFacade.deltaStepping(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> deltaSteppingWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                AllShortestPathsDeltaWriteConfig::of,
                writeModeFacade::deltaStepping
            )
        );
    }

    public Stream<MemoryEstimateResult> deltaSteppingWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            AllShortestPathsDeltaWriteConfig::of,
            configuration -> estimationModeFacade.deltaStepping(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public DepthFirstSearchMutateStub depthFirstSearchMutateStub() {
        return depthFirstSearchMutateStub;
    }

    public Stream<DfsStreamResult> depthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new DfsStreamResultBuilder(nodeLookup, procedureReturnColumns.contains("path"));

        return runStreamAlgorithm(
            graphName,
            configuration,
            DfsStreamConfig::of,
            resultBuilder,
            streamModeFacade::depthFirstSearch
        );
    }

    public Stream<MemoryEstimateResult> depthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            DfsStreamConfig::of,
            configuration -> estimationModeFacade.depthFirstSearch(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<KSpanningTreeWriteResult> kSpanningTreeWrite(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new KSpanningTreeResultBuilderForWriteMode();

        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                KSpanningTreeWriteConfig::of,
                writeModeFacade::kSpanningTree,
                resultBuilder
            )
        );
    }

    public Stream<PathFindingStreamResult> longestPathStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<DagLongestPathStreamConfig>(
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return runStreamAlgorithm(
            graphName,
            configuration,
            DagLongestPathStreamConfig::of,
            resultBuilder,
            streamModeFacade::longestPath
        );
    }

    public Stream<StandardModeResult> randomWalkStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new RandomWalkResultBuilderForStatsMode();

        return runStatsAlgorithm(
            graphName,
            configuration,
            RandomWalkStatsConfig::of,
            resultBuilder,
            statsModeFacade::randomWalk
        );
    }

    public Stream<MemoryEstimateResult> randomWalkStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            RandomWalkStatsConfig::of,
            configuration -> estimationModeFacade.randomWalk(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<RandomWalkStreamResult> randomWalkStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new RandomWalkResultBuilderForStreamMode(
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return runStreamAlgorithm(
            graphName,
            configuration,
            RandomWalkStreamConfig::of,
            resultBuilder,
            streamModeFacade::randomWalk
        );
    }

    public Stream<MemoryEstimateResult> randomWalkStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            RandomWalkStreamConfig::of,
            configuration -> estimationModeFacade.randomWalk(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub() {
        return singlePairShortestPathAStarMutateStub;
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathAStarStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            ShortestPathAStarStreamConfig::of,
            streamModeFacade::singlePairShortestPathAStar
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathAStarStreamConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathAStar(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathAStarWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                ShortestPathAStarWriteConfig::of,
                writeModeFacade::singlePairShortestPathAStar
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathAStarWriteConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathAStar(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub() {
        return singlePairShortestPathDijkstraMutateStub;
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            ShortestPathDijkstraStreamConfig::of,
            streamModeFacade::singlePairShortestPathDijkstra
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathDijkstraStreamConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathDijkstra(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                ShortestPathDijkstraWriteConfig::of,
                writeModeFacade::singlePairShortestPathDijkstra
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathDijkstraWriteConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathDijkstra(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub() {
        return singlePairShortestPathYensMutateStub;
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathYensStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            ShortestPathYensStreamConfig::of,
            streamModeFacade::singlePairShortestPathYens
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathYensStreamConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathYens(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathYensWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                ShortestPathYensWriteConfig::of,
                writeModeFacade::singlePairShortestPathYens
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            ShortestPathYensWriteConfig::of,
            configuration -> estimationModeFacade.singlePairShortestPathYens(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub() {
        return singleSourceShortestPathDijkstraMutateStub;
    }

    public Stream<PathFindingStreamResult> singleSourceShortestPathDijkstraStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            AllShortestPathsDijkstraStreamConfig::of,
            streamModeFacade::singleSourceShortestPathDijkstra
        );
    }

    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            AllShortestPathsDijkstraStreamConfig::of,
            configuration -> estimationModeFacade.singleSourceShortestPathDijkstra(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<StandardWriteRelationshipsResult> singleSourceShortestPathDijkstraWrite(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                AllShortestPathsDijkstraWriteConfig::of,
                writeModeFacade::singleSourceShortestPathDijkstra
            )
        );
    }

    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            AllShortestPathsDijkstraWriteConfig::of,
            configuration -> estimationModeFacade.singleSourceShortestPathDijkstra(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SpanningTreeMutateStub spanningTreeMutateStub() {
        return spanningTreeMutateStub;
    }

    public Stream<SpanningTreeStatsResult> spanningTreeStats(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new SpanningTreeResultBuilderForStatsMode();

        return runStatsAlgorithm(
            graphName,
            configuration,
            SpanningTreeStatsConfig::of,
            resultBuilder,
            statsModeFacade::spanningTree
        );
    }

    public Stream<MemoryEstimateResult> spanningTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            SpanningTreeStatsConfig::of,
            configuration -> estimationModeFacade.spanningTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SpanningTreeStreamResult> spanningTreeStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SpanningTreeResultBuilderForStreamMode();

        return runStreamAlgorithm(
            graphName,
            configuration,
            SpanningTreeStreamConfig::of,
            resultBuilder,
            streamModeFacade::spanningTree
        );
    }

    public Stream<MemoryEstimateResult> spanningTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            SpanningTreeStreamConfig::of,
            configuration -> estimationModeFacade.spanningTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SpanningTreeWriteResult> spanningTreeWrite(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SpanningTreeResultBuilderForWriteMode();

        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                SpanningTreeWriteConfig::of,
                writeModeFacade::spanningTree,
                resultBuilder
            )
        );
    }

    public Stream<MemoryEstimateResult> spanningTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            SpanningTreeWriteConfig::of,
            configuration -> estimationModeFacade.spanningTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public SteinerTreeMutateStub steinerTreeMutateStub() {
        return steinerTreeMutateStub;
    }

    public Stream<SteinerStatsResult> steinerTreeStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SteinerTreeResultBuilderForStatsMode();

        return runStatsAlgorithm(
            graphName,
            configuration,
            SteinerTreeStatsConfig::of,
            resultBuilder,
            statsModeFacade::steinerTree
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            SteinerTreeStatsConfig::of,
            configuration -> estimationModeFacade.steinerTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SteinerTreeStreamResult> steinerTreeStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SteinerTreeResultBuilderForStreamMode();

        return runStreamAlgorithm(
            graphName,
            configuration,
            SteinerTreeStreamConfig::of,
            resultBuilder,
            streamModeFacade::steinerTree
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            SteinerTreeStreamConfig::of,
            configuration -> estimationModeFacade.steinerTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SteinerWriteResult> steinerTreeWrite(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SteinerTreeResultBuilderForWriteMode();

        return Stream.of(
            runWriteAlgorithm(
                graphName,
                configuration,
                SteinerTreeWriteConfig::of,
                writeModeFacade::steinerTree,
                resultBuilder
            )
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = runEstimation(
            algorithmConfiguration,
            SteinerTreeWriteConfig::of,
            configuration -> estimationModeFacade.steinerTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<TopologicalSortStreamResult> topologicalSortStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        var resultBuilder = new TopologicalSortResultBuilderForStreamMode();

        return runStreamAlgorithm(
            graphName,
            configuration,
            TopologicalSortStreamConfig::of,
            resultBuilder,
            streamModeFacade::topologicalSort
        );
    }

    /**
     * Just a bit of reuse
     */
    private <CONFIGURATION extends AlgoBaseConfig> MemoryEstimateResult runEstimation(
        Map<String, Object> algorithmConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationParser,
        Function<CONFIGURATION, MemoryEstimateResult> supplier
    ) {
        var configuration = configurationCreator.createConfiguration(
            algorithmConfiguration,
            configurationParser
        );

        return supplier.apply(configuration);
    }

    /**
     * A*, Dijkstra, Yens all share the same result builder
     */
    private <CONFIGURATION extends AlgoBaseConfig> Stream<PathFindingStreamResult> runPathOrientedAlgorithmInStreamMode(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, PathFindingResult, Stream<PathFindingStreamResult>> algorithm
    ) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<CONFIGURATION>(
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return runStreamAlgorithm(graphNameAsString, rawConfiguration, configurationSupplier, resultBuilder, algorithm);
    }

    private <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> Stream<RESULT_TO_CALLER> runStatsAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>> resultBuilder,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>> algorithm
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfiguration(rawConfiguration, configurationSupplier);

        return algorithm.compute(graphName, configuration, resultBuilder);
    }

    /**
     * Some reuse, all the algorithms use the same high level structure:
     * <ol>
     *     <li> configuration parsing
     *     <li> parameter marshalling
     *     <li> delegating to down stream layer to call the thing we are actually interested in
     *     <li> handle resource closure
     * </ol>
     */
    private <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> Stream<RESULT_TO_CALLER> runStreamAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>> resultBuilder,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, Stream<RESULT_TO_CALLER>> algorithm
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfigurationForStream(rawConfiguration, configurationSupplier);

        var resultStream = algorithm.compute(graphName, configuration, resultBuilder);

        // we need to do this for stream mode
        closeableResourceRegistry.register(resultStream);

        return resultStream;
    }

    /**
     * A*, Dijkstra and Yens use the same variant of result builder
     */
    private <CONFIGURATION extends AlgoBaseConfig> StandardWriteRelationshipsResult runWriteAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, PathFindingResult, StandardWriteRelationshipsResult> algorithm
    ) {
        var resultBuilder = new PathFindingResultBuilderForWriteMode<CONFIGURATION>();

        return runWriteAlgorithm(
            graphNameAsString,
            rawConfiguration,
            configurationSupplier,
            algorithm,
            resultBuilder
        );
    }

    private <CONFIGURATION extends AlgoBaseConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> RESULT_TO_CALLER runWriteAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> algorithm,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
    ) {
        var graphName = GraphName.parse(graphNameAsString);
        var configuration = configurationCreator.createConfiguration(rawConfiguration, configurationSupplier);

        return algorithm.compute(graphName, configuration, resultBuilder);
    }
}
