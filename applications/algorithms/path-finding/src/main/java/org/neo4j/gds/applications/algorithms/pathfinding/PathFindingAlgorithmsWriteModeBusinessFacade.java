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
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.Computation;
import org.neo4j.gds.applications.algorithms.machinery.Label;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.WriteResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.WriteSideEffect;
import org.neo4j.gds.applications.algorithms.machinery.WriteStep;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.JobIdConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.kspanningtree.KSpanningTreeWriteConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowWriteConfig;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.mcmf.MCMFWriteConfig;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.pathfinding.BellmanFordWriteStep;
import org.neo4j.gds.pathfinding.KSpanningTreeWriteStep;
import org.neo4j.gds.pathfinding.MCMFWriteStep;
import org.neo4j.gds.pathfinding.MaxFlowWriteStep;
import org.neo4j.gds.pathfinding.PrizeCollectingSteinerTreeWriteStep;
import org.neo4j.gds.pathfinding.ShortestPathWriteStep;
import org.neo4j.gds.pathfinding.SpanningTreeWriteStep;
import org.neo4j.gds.pathfinding.SteinerTreeWriteStep;
import org.neo4j.gds.paths.WritePathOptionsConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordWriteConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.pcst.PCSTWriteConfig;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;

import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.KSpanningTree;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.PCST;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.SingleSourceDijkstra;

/**
 * Here is the top level business facade for all your path finding write needs.
 * It will have all pathfinding algorithms on it, in write mode.
 */
public class PathFindingAlgorithmsWriteModeBusinessFacade {
    private final Log log;

    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final RequestScopedDependencies requestScopedDependencies;
    private final WriteContext writeContext;
    private final WriteRelationshipService writeRelationshipService;
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithmsBusinessFacade pathFindingAlgorithms;
    private final InstrumentedPathFindingAlgorithms instrumentedPathFindingAlgorithms;
    private final Synchroniser synchroniser;

    PathFindingAlgorithmsWriteModeBusinessFacade(
        Log log,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext,
        WriteRelationshipService writeRelationshipService,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithmsBusinessFacade pathFindingAlgorithms,
        InstrumentedPathFindingAlgorithms instrumentedPathFindingAlgorithms,
        Synchroniser synchroniser
    ) {
        this.log = log;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.requestScopedDependencies = requestScopedDependencies;
        this.writeContext = writeContext;
        this.writeRelationshipService = writeRelationshipService;
        this.estimationFacade = estimationFacade;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
        this.instrumentedPathFindingAlgorithms = instrumentedPathFindingAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> RESULT bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordWriteConfig configuration,
        ResultBuilder<AllShortestPathsBellmanFordWriteConfig, BellmanFordResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new BellmanFordWriteStep(
            writeRelationshipService,
            configuration.writeRelationshipType(),
            configuration.writeNegativeCycles(),
            configuration.writeNodeIds(),
            configuration.writeCosts(),
            configuration::resolveResultStore,
            configuration.jobId()
        );

        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.bellmanFord(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaWriteConfig configuration,
        ResultBuilder<AllShortestPathsDeltaWriteConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = createShortestPathWriteStep(configuration);

        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.deltaStepping(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT kSpanningTree(
        GraphName graphName,
        KSpanningTreeWriteConfig configuration,
        ResultBuilder<KSpanningTreeWriteConfig, SpanningTree, RESULT, Void> resultBuilder
    ) {
        var writeStep = new KSpanningTreeWriteStep(
            log,
            requestScopedDependencies,
            configuration.writeProperty(),
            writeContext,
            configuration::resolveResultStore,
            configuration.jobId(),
            configuration.writeConcurrency()
        );

        return runAlgorithmAndWrite(
            graphName,
            configuration,
            KSpanningTree,
            estimationFacade::kSpanningTree,
            (graph, __) -> pathFindingAlgorithms.kSpanningTree(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT maxFlow(
        GraphName graphName,
        MaxFlowWriteConfig configuration,
        ResultBuilder<MaxFlowWriteConfig, FlowResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new MaxFlowWriteStep(
            writeRelationshipService,
            configuration.writeRelationshipType(),
            configuration.writeProperty(),
            configuration::resolveResultStore,
            configuration.jobId()
        );

        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.maxFlow(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT minCostMaxFlow(
        GraphName graphName,
        MCMFWriteConfig configuration,
        ResultBuilder<MCMFWriteConfig, CostFlowResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new MCMFWriteStep(
            writeRelationshipService,
            configuration.writeRelationshipType(),
            configuration.writeProperty(),
            configuration::resolveResultStore,
            configuration.jobId()
        );

        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.minCostMaxFlow(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT pcst(
        GraphName graphName,
        PCSTWriteConfig configuration,
        ResultBuilder<PCSTWriteConfig, PrizeSteinerTreeResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new PrizeCollectingSteinerTreeWriteStep(
            writeRelationshipService,
            configuration.writeRelationshipType(),
            configuration.writeProperty(),
            configuration::resolveResultStore,
            configuration.jobId()
        );

        return runAlgorithmAndWrite(
            graphName,
            configuration,
            PCST,
            estimationFacade::pcst,
            (graph, __) -> pathFindingAlgorithms.pcst(graph, configuration),
            writeStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathAStar(
        GraphName graphName,
        ShortestPathAStarWriteConfig configuration,
        ResultBuilder<ShortestPathAStarWriteConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = createShortestPathWriteStep(configuration);

        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.singlePairShortestPathAStar(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT singlePairShortestPathDijkstra(
        GraphName graphName,
        ShortestPathDijkstraWriteConfig configuration,
        ResultBuilder<ShortestPathDijkstraWriteConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = createShortestPathWriteStep(configuration);

        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.singlePairShortestPathDijkstra(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT singlePairShortestPathYens(
        GraphName graphName,
        ShortestPathYensWriteConfig configuration,
        ResultBuilder<ShortestPathYensWriteConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = createShortestPathWriteStep(configuration);

        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.singlePairShortestPathYens(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
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
            (graph, __) -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT spanningTree(
        GraphName graphName,
        SpanningTreeWriteConfig configuration,
        ResultBuilder<SpanningTreeWriteConfig, SpanningTree, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new SpanningTreeWriteStep(
            writeRelationshipService,
            configuration.writeRelationshipType(),
            configuration.writeProperty(),
            configuration::resolveResultStore,
            configuration.jobId()
        );

        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.spanningTree(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    public <RESULT> RESULT steinerTree(
        GraphName graphName,
        SteinerTreeWriteConfig configuration,
        ResultBuilder<SteinerTreeWriteConfig, SteinerTreeResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = new SteinerTreeWriteStep(
            writeRelationshipService,
            configuration.sourceNode(),
            configuration.writeRelationshipType(),
            configuration.writeProperty(),
            configuration::resolveResultStore,
            configuration.jobId()
        );

        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.steinerTree(
            graphName,
            configuration,
            Optional.of(new WriteSideEffect<>(configuration.jobId(), writeStep)),
            new WriteResultRenderer<>(configuration, resultBuilder)
        ));
    }

    /**
     * A*, Dijkstra and Yens use the same variant of write step
     */
    private <CONFIGURATION extends AlgoBaseConfig & RelationshipWeightConfig & WriteRelationshipConfig & WritePathOptionsConfig, RESULT_TO_CALLER> RESULT_TO_CALLER runAlgorithmAndWrite(
        GraphName graphName,
        CONFIGURATION configuration,
        Label label,
        Supplier<MemoryEstimation> memoryEstimation,
        Computation<PathFindingResult> algorithm,
        ResultBuilder<CONFIGURATION, PathFindingResult, RESULT_TO_CALLER, RelationshipsWritten> resultBuilder
    ) {
        var writeStep = createShortestPathWriteStep(configuration);

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
        Label label,
        Supplier<MemoryEstimation> memoryEstimation,
        Computation<RESULT_FROM_ALGORITHM> algorithm,
        WriteStep<RESULT_FROM_ALGORITHM, MUTATE_OR_WRITE_METADATA> writeStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER, MUTATE_OR_WRITE_METADATA> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInWriteMode(
            graphName,
            configuration,
            label,
            memoryEstimation,
            algorithm,
            writeStep,
            resultBuilder
        );
    }

    private <CONFIGURATION extends JobIdConfig & WriteRelationshipConfig & WritePathOptionsConfig> ShortestPathWriteStep createShortestPathWriteStep(
        CONFIGURATION configuration
    ) {
        return new ShortestPathWriteStep(
            writeRelationshipService,
            configuration.writeRelationshipType(),
            configuration.writeNodeIds(),
            configuration.writeCosts(),
            configuration::resolveResultStore,
            configuration.jobId()
        );
    }
}
