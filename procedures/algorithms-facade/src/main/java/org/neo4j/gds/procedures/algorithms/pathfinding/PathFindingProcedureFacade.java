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
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.applications.ApplicationsFacade;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.applications.algorithms.machinery.StreamResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
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
import org.neo4j.gds.procedures.algorithms.AlgorithmHandle;
import org.neo4j.gds.procedures.algorithms.StreamAlgorithmHandle;
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
import org.neo4j.gds.procedures.algorithms.runners.AlgorithmExecutionScaffolding;
import org.neo4j.gds.procedures.algorithms.runners.EstimationModeRunner;
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
    private final EstimationModeRunner estimationMode;
    private final AlgorithmExecutionScaffolding algorithmExecutionScaffolding;

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
        DepthFirstSearchMutateStub depthFirstSearchMutateStub, RandomWalkMutateStub randomWalkMutateStub,
        SinglePairShortestPathAStarMutateStub singlePairShortestPathAStarMutateStub,
        SinglePairShortestPathDijkstraMutateStub singlePairShortestPathDijkstraMutateStub,
        SinglePairShortestPathYensMutateStub singlePairShortestPathYensMutateStub,
        SingleSourceShortestPathDijkstraMutateStub singleSourceShortestPathDijkstraMutateStub,
        SpanningTreeMutateStub spanningTreeMutateStub,
        SteinerTreeMutateStub steinerTreeMutateStub,
        EstimationModeRunner estimationMode,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding
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

        this.estimationMode = estimationMode;
        this.algorithmExecutionScaffolding = algorithmExecutionScaffolding;
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
        EstimationModeRunner estimationModeRunner,
        AlgorithmExecutionScaffolding algorithmExecutionScaffolding
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
            spanningTreeMutateStub, steinerTreeMutateStub, estimationModeRunner, algorithmExecutionScaffolding
        );
    }

    public Stream<AllShortestPathsStreamResult> allShortestPathStream(
        String graphName,
        Map<String, Object> configuration
    ) {
        StreamResultBuilder<AllShortestPathsConfig, Stream<AllShortestPathsStreamResult>, AllShortestPathsStreamResult> resultBuilder =
            (g, gs, c, result) -> result.orElse(Stream.empty());

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            AllShortestPathsConfig::of,
            streamModeBusinessFacade::allShortestPaths,
            resultBuilder
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

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            AllShortestPathsBellmanFordStreamConfig::of,
            streamModeBusinessFacade::bellmanFord,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> bellmanFordStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            AllShortestPathsBellmanFordStreamConfig::of,
            configuration -> estimationModeBusinessFacade.bellmanFord(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<BellmanFordStatsResult> bellmanFordStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new BellmanFordResultBuilderForStatsMode();

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            AllShortestPathsBellmanFordStatsConfig::of,
            statsModeBusinessFacade::bellmanFord,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> bellmanFordStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            AllShortestPathsBellmanFordStatsConfig::of,
            configuration -> estimationModeBusinessFacade.bellmanFord(
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
            algorithmExecutionScaffolding.runAlgorithm(
                graphName,
                configuration,
                AllShortestPathsBellmanFordWriteConfig::of,
                writeModeBusinessFacade::bellmanFord,
                resultBuilder
            )
        );
    }

    public Stream<MemoryEstimateResult> bellmanFordWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            AllShortestPathsBellmanFordWriteConfig::of,
            configuration -> estimationModeBusinessFacade.bellmanFord(
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            BfsStatsConfig::of,
            statsModeBusinessFacade::breadthFirstSearch,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> breadthFirstSearchStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            BfsStatsConfig::of,
            configuration -> estimationModeBusinessFacade.breadthFirstSearch(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<BfsStreamResult> breadthFirstSearchStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new BfsStreamResultBuilder(nodeLookup, procedureReturnColumns.contains("path"));

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            BfsStreamConfig::of,
            streamModeBusinessFacade::breadthFirstSearch,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> breadthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            BfsStreamConfig::of,
            configuration -> estimationModeBusinessFacade.breadthFirstSearch(
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            AllShortestPathsDeltaStatsConfig::of,
            statsModeBusinessFacade::deltaStepping,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> deltaSteppingStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            AllShortestPathsDeltaStatsConfig::of,
            configuration -> estimationModeBusinessFacade.deltaStepping(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<PathFindingStreamResult> deltaSteppingStream(String graphName, Map<String, Object> configuration) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            AllShortestPathsDeltaStreamConfig::of,
            streamModeBusinessFacade::deltaStepping
        );
    }

    public Stream<MemoryEstimateResult> deltaSteppingStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            AllShortestPathsDeltaStreamConfig::of,
            configuration -> estimationModeBusinessFacade.deltaStepping(
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
                writeModeBusinessFacade::deltaStepping
            )
        );
    }

    public Stream<MemoryEstimateResult> deltaSteppingWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            AllShortestPathsDeltaWriteConfig::of,
            configuration -> estimationModeBusinessFacade.deltaStepping(
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

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            DfsStreamConfig::of,
            streamModeBusinessFacade::depthFirstSearch,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> depthFirstSearchStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            DfsStreamConfig::of,
            configuration -> estimationModeBusinessFacade.depthFirstSearch(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<KSpanningTreeWriteResult> kSpanningTreeWrite(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new KSpanningTreeResultBuilderForWriteMode();

        return Stream.of(
            algorithmExecutionScaffolding.runAlgorithm(
                graphName,
                configuration,
                KSpanningTreeWriteConfig::of,
                writeModeBusinessFacade::kSpanningTree,
                resultBuilder
            )
        );
    }

    public Stream<PathFindingStreamResult> longestPathStream(String graphName, Map<String, Object> configuration) {
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            DagLongestPathStreamConfig::of,
            streamModeBusinessFacade::longestPath
        );
    }

    public Stream<StandardModeResult> randomWalkStats(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new RandomWalkResultBuilderForStatsMode();

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            RandomWalkStatsConfig::of,
            statsModeBusinessFacade::randomWalk,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> randomWalkStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            RandomWalkStatsConfig::of,
            configuration -> estimationModeBusinessFacade.randomWalk(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<RandomWalkStreamResult> randomWalkStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new RandomWalkResultBuilderForStreamMode(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            RandomWalkStreamConfig::of,
            streamModeBusinessFacade::randomWalk,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> randomWalkStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            RandomWalkStreamConfig::of,
            configuration -> estimationModeBusinessFacade.randomWalk(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
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
        return runPathOrientedAlgorithmInStreamMode(
            graphName,
            configuration,
            ShortestPathAStarStreamConfig::of,
            streamModeBusinessFacade::singlePairShortestPathAStar
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ShortestPathAStarStreamConfig::of,
            configuration -> estimationModeBusinessFacade.singlePairShortestPathAStar(
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
                writeModeBusinessFacade::singlePairShortestPathAStar
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathAStarWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ShortestPathAStarWriteConfig::of,
            configuration -> estimationModeBusinessFacade.singlePairShortestPathAStar(
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
            streamModeBusinessFacade::singlePairShortestPathDijkstra
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ShortestPathDijkstraStreamConfig::of,
            configuration -> estimationModeBusinessFacade.singlePairShortestPathDijkstra(
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
                writeModeBusinessFacade::singlePairShortestPathDijkstra
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ShortestPathDijkstraWriteConfig::of,
            configuration -> estimationModeBusinessFacade.singlePairShortestPathDijkstra(
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
            streamModeBusinessFacade::singlePairShortestPathYens
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ShortestPathYensStreamConfig::of,
            configuration -> estimationModeBusinessFacade.singlePairShortestPathYens(
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
                writeModeBusinessFacade::singlePairShortestPathYens
            )
        );
    }

    public Stream<MemoryEstimateResult> singlePairShortestPathYensWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            ShortestPathYensWriteConfig::of,
            configuration -> estimationModeBusinessFacade.singlePairShortestPathYens(
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
            streamModeBusinessFacade::singleSourceShortestPathDijkstra
        );
    }

    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            AllShortestPathsDijkstraStreamConfig::of,
            configuration -> estimationModeBusinessFacade.singleSourceShortestPathDijkstra(
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
                writeModeBusinessFacade::singleSourceShortestPathDijkstra
            )
        );
    }

    public Stream<MemoryEstimateResult> singleSourceShortestPathDijkstraWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            AllShortestPathsDijkstraWriteConfig::of,
            configuration -> estimationModeBusinessFacade.singleSourceShortestPathDijkstra(
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            SpanningTreeStatsConfig::of,
            statsModeBusinessFacade::spanningTree,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> spanningTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            SpanningTreeStatsConfig::of,
            configuration -> estimationModeBusinessFacade.spanningTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SpanningTreeStreamResult> spanningTreeStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SpanningTreeResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            SpanningTreeStreamConfig::of,
            streamModeBusinessFacade::spanningTree,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> spanningTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            SpanningTreeStreamConfig::of,
            configuration -> estimationModeBusinessFacade.spanningTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SpanningTreeWriteResult> spanningTreeWrite(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SpanningTreeResultBuilderForWriteMode();

        return Stream.of(
            algorithmExecutionScaffolding.runAlgorithm(
                graphName,
                configuration,
                SpanningTreeWriteConfig::of,
                writeModeBusinessFacade::spanningTree,
                resultBuilder
            )
        );
    }

    public Stream<MemoryEstimateResult> spanningTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            SpanningTreeWriteConfig::of,
            configuration -> estimationModeBusinessFacade.spanningTree(
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

        return algorithmExecutionScaffolding.runStatsAlgorithm(
            graphName,
            configuration,
            SteinerTreeStatsConfig::of,
            statsModeBusinessFacade::steinerTree,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeStatsEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            SteinerTreeStatsConfig::of,
            configuration -> estimationModeBusinessFacade.steinerTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SteinerTreeStreamResult> steinerTreeStream(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SteinerTreeResultBuilderForStreamMode();

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            SteinerTreeStreamConfig::of,
            streamModeBusinessFacade::steinerTree,
            resultBuilder
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeStreamEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            SteinerTreeStreamConfig::of,
            configuration -> estimationModeBusinessFacade.steinerTree(
                configuration,
                graphNameOrConfiguration
            )
        );

        return Stream.of(result);
    }

    public Stream<SteinerWriteResult> steinerTreeWrite(String graphName, Map<String, Object> configuration) {
        var resultBuilder = new SteinerTreeResultBuilderForWriteMode();

        return Stream.of(
            algorithmExecutionScaffolding.runAlgorithm(
                graphName,
                configuration,
                SteinerTreeWriteConfig::of,
                writeModeBusinessFacade::steinerTree,
                resultBuilder
            )
        );
    }

    public Stream<MemoryEstimateResult> steinerTreeWriteEstimate(
        Object graphNameOrConfiguration,
        Map<String, Object> algorithmConfiguration
    ) {
        var result = estimationMode.runEstimation(
            algorithmConfiguration,
            SteinerTreeWriteConfig::of,
            configuration -> estimationModeBusinessFacade.steinerTree(
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

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphName,
            configuration,
            TopologicalSortStreamConfig::of,
            streamModeBusinessFacade::topologicalSort,
            resultBuilder
        );
    }

    /**
     * A*, Dijkstra, Yens all share the same result builder
     */
    private <CONFIGURATION extends AlgoBaseConfig> Stream<PathFindingStreamResult> runPathOrientedAlgorithmInStreamMode(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        StreamAlgorithmHandle<CONFIGURATION, PathFindingResult, PathFindingStreamResult> algorithm
    ) {
        var resultBuilder = new PathFindingResultBuilderForStreamMode<CONFIGURATION>(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return algorithmExecutionScaffolding.runStreamAlgorithm(
            graphNameAsString,
            rawConfiguration,
            configurationSupplier,
            algorithm,
            resultBuilder
        );
    }

    /**
     * A*, Dijkstra and Yens use the same variant of result builder
     */
    private <CONFIGURATION extends AlgoBaseConfig> StandardWriteRelationshipsResult runWriteAlgorithm(
        String graphNameAsString,
        Map<String, Object> rawConfiguration,
        Function<CypherMapWrapper, CONFIGURATION> configurationSupplier,
        AlgorithmHandle<CONFIGURATION, PathFindingResult, StandardWriteRelationshipsResult, RelationshipsWritten> algorithm
    ) {
        var resultBuilder = new PathFindingResultBuilderForWriteMode<CONFIGURATION>();

        return algorithmExecutionScaffolding.runAlgorithm(
            graphNameAsString,
            rawConfiguration,
            configurationSupplier,
            algorithm,
            resultBuilder
        );
    }
}
