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
package org.neo4j.gds.applications.algorithms.pathfinding;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmComputation;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MutateOrWriteStep;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.kspanningtree.KSpanningTreeWriteConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.paths.WritePathOptionsConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.bellmanford.BellmanFordWriteConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;

import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.AStar;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BellmanFord;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.DeltaStepping;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Dijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.KSpanningTree;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.SingleSourceDijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.SteinerTree;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Yens;

/**
 * Here is the top level business facade for all your path finding write needs.
 * It will have all pathfinding algorithms on it, in write mode.
 */
public class PathFindingAlgorithmsWriteModeBusinessFacade {
    private final Log log;

    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;
    private final RequestScopedDependencies requestScopedDependencies;

    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithms pathFindingAlgorithms;

    public PathFindingAlgorithmsWriteModeBusinessFacade(
        Log log,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        RequestScopedDependencies requestScopedDependencies,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithms pathFindingAlgorithms
    ) {
        this.log = log;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.requestScopedDependencies = requestScopedDependencies;
        this.estimationFacade = estimationFacade;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
    }

    public <RESULT> RESULT bellmanFord(
        GraphName graphName,
        BellmanFordWriteConfig configuration,
        ResultBuilder<BellmanFordWriteConfig, BellmanFordResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new BellmanFordWriteStep(
            log,
            requestScopedDependencies,
            configuration
        );

        return runAlgorithmAndWrite(
            graphName,
            configuration,
            BellmanFord,
            () -> estimationFacade.bellmanFord(configuration),
            graph -> pathFindingAlgorithms.bellmanFord(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaWriteConfig configuration,
        ResultBuilder<AllShortestPathsDeltaWriteConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            DeltaStepping,
            estimationFacade::deltaStepping,
            graph -> pathFindingAlgorithms.deltaStepping(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT kSpanningTree(
        GraphName graphName,
        KSpanningTreeWriteConfig configuration,
        ResultBuilder<KSpanningTreeWriteConfig, SpanningTree, RESULT, Void> resultBuilder
    ) {
        var writeStep = new KSpanningTreeWriteStep(
            log,
            requestScopedDependencies,
            configuration
        );

        return runAlgorithmAndWrite(
            graphName,
            configuration,
            KSpanningTree,
            estimationFacade::kSpanningTree,
            graph -> pathFindingAlgorithms.kSpanningTree(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathAStar(
        GraphName graphName,
        ShortestPathAStarWriteConfig configuration,
        ResultBuilder<ShortestPathAStarWriteConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            AStar,
            estimationFacade::singlePairShortestPathAStar,
            graph -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathDijkstra(
        GraphName graphName,
        ShortestPathDijkstraWriteConfig configuration,
        ResultBuilder<ShortestPathDijkstraWriteConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            Dijkstra,
            () -> estimationFacade.singlePairShortestPathDijkstra(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathYens(
        GraphName graphName,
        ShortestPathYensWriteConfig configuration,
        ResultBuilder<ShortestPathYensWriteConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            Yens,
            () -> estimationFacade.singlePairShortestPathYens(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathYens(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singleSourceShortestPathDijkstra(
        GraphName graphName,
        AllShortestPathsDijkstraWriteConfig configuration,
        ResultBuilder<AllShortestPathsDijkstraWriteConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            SingleSourceDijkstra,
            () -> estimationFacade.singleSourceShortestPathDijkstra(configuration),
            graph -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT spanningTree(
        GraphName graphName,
        SpanningTreeWriteConfig configuration,
        ResultBuilder<SpanningTreeWriteConfig, SpanningTree, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new SpanningTreeWriteStep(
            log,
            requestScopedDependencies,
            configuration
        );

        return runAlgorithmAndWrite(
            graphName,
            configuration,
            LabelForProgressTracking.SpanningTree,
            estimationFacade::spanningTree,
            graph -> pathFindingAlgorithms.spanningTree(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT steinerTree(
        GraphName graphName,
        SteinerTreeWriteConfig configuration,
        ResultBuilder<SteinerTreeWriteConfig, SteinerTreeResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new SteinerTreeWriteStep(requestScopedDependencies, configuration);

        return runAlgorithmAndWrite(
            graphName,
            configuration,
            SteinerTree,
            () -> estimationFacade.steinerTree(configuration),
            graph -> pathFindingAlgorithms.steinerTree(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    /**
     * A*, Dijkstra and Yens use the same variant of write step
     */
    private <CONFIGURATION extends AlgoBaseConfig & RelationshipWeightConfig & WriteRelationshipConfig & WritePathOptionsConfig, RESULT_TO_CALLER> RESULT_TO_CALLER runAlgorithmAndWrite(
        GraphName graphName,
        CONFIGURATION configuration,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> memoryEstimation,
        AlgorithmComputation<PathFindingResult> algorithm,
        ResultBuilder<CONFIGURATION, PathFindingResult, RESULT_TO_CALLER, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new ShortestPathWriteStep<>(
            log,
            requestScopedDependencies,
            configuration
        );

        return runAlgorithmAndWrite(
            graphName,
            configuration,
            label,
            memoryEstimation,
            algorithm,
            writeStep,
            resultBuilder
        );
    }

    private <CONFIGURATION extends AlgoBaseConfig & RelationshipWeightConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> RESULT_TO_CALLER runAlgorithmAndWrite(
        GraphName graphName,
        CONFIGURATION configuration,
        LabelForProgressTracking label,
        Supplier<MemoryEstimation> memoryEstimation,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithm,
        MutateOrWriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> writeStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            label,
            memoryEstimation,
            algorithm,
            Optional.of(writeStep),
            resultBuilder
        );
    }
}
