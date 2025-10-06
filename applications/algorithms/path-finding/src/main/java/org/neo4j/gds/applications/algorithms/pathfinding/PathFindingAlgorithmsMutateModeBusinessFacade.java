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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplate;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.DimensionTransformer;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateRelationshipService;
import org.neo4j.gds.applications.algorithms.machinery.MutateResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowMutateConfig;
import org.neo4j.gds.pathfinding.BellmanFordMutateStep;
import org.neo4j.gds.pathfinding.MaxFlowMutateStep;
import org.neo4j.gds.pathfinding.PrizeCollectingSteinerTreeMutateStep;
import org.neo4j.gds.pathfinding.RandomWalkCountingNodeVisitsMutateStep;
import org.neo4j.gds.pathfinding.SearchMutateStep;
import org.neo4j.gds.pathfinding.ShortestPathMutateStep;
import org.neo4j.gds.pathfinding.SpanningTreeMutateStep;
import org.neo4j.gds.pathfinding.SteinerTreeMutateStep;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarMutateConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordMutateConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaMutateConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.paths.traverse.BfsMutateConfig;
import org.neo4j.gds.paths.traverse.DfsMutateConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;
import org.neo4j.gds.pcst.PCSTMutateConfig;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkMutateConfig;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.AStar;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BFS;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BellmanFord;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.DFS;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.DeltaStepping;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Dijkstra;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.MaxFlow;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.RandomWalk;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.SingleSourceDijkstra;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.SteinerTree;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.Yens;

/**
 * Here is the top level business facade for all your path finding mutate needs.
 * It will have all pathfinding algorithms on it, in mutate mode.
 */
public class PathFindingAlgorithmsMutateModeBusinessFacade {
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithmsBusinessFacade pathFindingAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;
    private final MutateNodePropertyService mutateNodeProperty;
    private final MutateRelationshipService mutateRelationshipService;

    public PathFindingAlgorithmsMutateModeBusinessFacade(
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithmsBusinessFacade pathFindingAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        MutateNodePropertyService mutateNodeProperty,
        MutateRelationshipService mutateRelationshipService
    ) {
        this.pathFindingAlgorithms = pathFindingAlgorithms;
        this.estimationFacade = estimationFacade;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.mutateNodeProperty = mutateNodeProperty;
        this.mutateRelationshipService = mutateRelationshipService;
    }

    public <RESULT> RESULT bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordMutateConfig configuration,
        ResultBuilder<AllShortestPathsBellmanFordMutateConfig, BellmanFordResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new BellmanFordMutateStep(
            configuration.mutateRelationshipType(),
            configuration.mutateNegativeCycles(),
            mutateRelationshipService
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            BellmanFord,
            () -> estimationFacade.bellmanFord(configuration),
            (graph, __) -> pathFindingAlgorithms.bellmanFord(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT breadthFirstSearch(
        GraphName graphName,
        BfsMutateConfig configuration,
        ResultBuilder<BfsMutateConfig, HugeLongArray, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new SearchMutateStep(
            mutateRelationshipService,
            configuration.mutateRelationshipType()
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            BFS,
            estimationFacade::breadthFirstSearch,
            (graph, __) -> pathFindingAlgorithms.breadthFirstSearch(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaMutateConfig configuration,
        ResultBuilder<AllShortestPathsDeltaMutateConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep(configuration.mutateRelationshipType(), mutateRelationshipService);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            DeltaStepping,
            estimationFacade::deltaStepping,
            (graph, __) -> pathFindingAlgorithms.deltaStepping(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT depthFirstSearch(
        GraphName graphName,
        DfsMutateConfig configuration,
        ResultBuilder<DfsMutateConfig, HugeLongArray, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new SearchMutateStep(
            mutateRelationshipService,
            configuration.mutateRelationshipType()
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            DFS,
            estimationFacade::depthFirstSearch,
            (graph, __) -> pathFindingAlgorithms.depthFirstSearch(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT maxFlow(
        GraphName graphName,
        MaxFlowMutateConfig configuration,
        ResultBuilder<MaxFlowMutateConfig, FlowResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new MaxFlowMutateStep(
            configuration.mutateRelationshipType(),
            configuration.mutateProperty(),
            mutateRelationshipService
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            MaxFlow,
            () -> estimationFacade.maxFlow(configuration),
            (graph, __) -> pathFindingAlgorithms.maxFlow(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT pcst(
        GraphName graphName,
        PCSTMutateConfig configuration,
        ResultBuilder<PCSTMutateConfig, PrizeSteinerTreeResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new PrizeCollectingSteinerTreeMutateStep(
            configuration.mutateRelationshipType(),
            configuration.mutateProperty(),
            mutateRelationshipService
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            SteinerTree,
            estimationFacade::pcst,
            (graph, __) -> pathFindingAlgorithms.pcst(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT randomWalk(
        GraphName graphName,
        RandomWalkMutateConfig configuration,
        ResultBuilder<RandomWalkMutateConfig, HugeAtomicLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new RandomWalkCountingNodeVisitsMutateStep(
            configuration.nodeLabels(),
            configuration.mutateProperty(),
            mutateNodeProperty
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            RandomWalk,
            // TODO: maybe have different memory estimation
            () -> estimationFacade.randomWalk(configuration),
            (graph, __) -> pathFindingAlgorithms.randomWalkCountingNodeVisits(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathAStar(
        GraphName graphName,
        ShortestPathAStarMutateConfig configuration,
        ResultBuilder<ShortestPathAStarMutateConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep(configuration.mutateRelationshipType(), mutateRelationshipService);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            AStar,
            estimationFacade::singlePairShortestPathAStar,
            (graph, __) -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathDijkstra(
        GraphName graphName,
        ShortestPathDijkstraMutateConfig configuration,
        ResultBuilder<ShortestPathDijkstraMutateConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep(configuration.mutateRelationshipType(), mutateRelationshipService);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            Dijkstra,
            () -> estimationFacade.singlePairShortestPathDijkstra(configuration),
            (graph, __) -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathDijkstraWithPaths(
        GraphName graphName,
        ShortestPathDijkstraMutateConfig configuration,
        Map<String, Stream<PathUsingInternalNodeIds>> pathStore,
        ResultBuilder<ShortestPathDijkstraMutateConfig, PathFindingResult, RESULT, Void> resultBuilder
    ) {
        var sideEffect = new StorePathsSideEffect(pathStore, configuration.mutateRelationshipType());

        return algorithmProcessingTemplate.processAlgorithmAndAnySideEffects(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            Optional.empty(),
            Dijkstra,
            DimensionTransformer.DISABLED,
            () -> estimationFacade.singlePairShortestPathDijkstra(configuration),
            (graph, __) -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            Optional.of(sideEffect),
            new MutateResultRenderer<>(configuration, resultBuilder)
        );
    }

    public <RESULT> RESULT singlePairShortestPathYens(
        GraphName graphName,
        ShortestPathYensMutateConfig configuration,
        ResultBuilder<ShortestPathYensMutateConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep(configuration.mutateRelationshipType(), mutateRelationshipService);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            Yens,
            () -> estimationFacade.singlePairShortestPathYens(configuration),
            (graph, __) -> pathFindingAlgorithms.singlePairShortestPathYens(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT singleSourceShortestPathDijkstra(
        GraphName graphName,
        AllShortestPathsDijkstraMutateConfig configuration,
        ResultBuilder<AllShortestPathsDijkstraMutateConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep(configuration.mutateRelationshipType(), mutateRelationshipService);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            SingleSourceDijkstra,
            () -> estimationFacade.singleSourceShortestPathDijkstra(configuration),
            (graph, __) -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT singleSourceShortestPathDijkstraWithPaths(
        GraphName graphName,
        AllShortestPathsDijkstraMutateConfig configuration,
        Map<String, Stream<PathUsingInternalNodeIds>> pathStore,
        ResultBuilder<AllShortestPathsDijkstraMutateConfig, PathFindingResult, RESULT, Void> resultBuilder
    ) {
        var sideEffect = new StorePathsSideEffect(pathStore, configuration.mutateRelationshipType());

        return algorithmProcessingTemplate.processAlgorithmAndAnySideEffects(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            Optional.empty(),
            SingleSourceDijkstra,
            DimensionTransformer.DISABLED,
            () -> estimationFacade.singleSourceShortestPathDijkstra(configuration),
            (graph, __) -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            Optional.of(sideEffect),
            new MutateResultRenderer<>(configuration, resultBuilder)
        );
    }


    public <RESULT> RESULT deltaSteppingWithPaths(
        GraphName graphName,
        AllShortestPathsDeltaMutateConfig configuration,
        Map<String, Stream<PathUsingInternalNodeIds>> pathStore,
        ResultBuilder<AllShortestPathsDeltaMutateConfig, PathFindingResult, RESULT, Void> resultBuilder
    ) {
        var sideEffect = new StorePathsSideEffect(pathStore, configuration.mutateRelationshipType());

        return algorithmProcessingTemplate.processAlgorithmAndAnySideEffects(
            Optional.empty(),
            graphName,
            configuration,
            Optional.empty(),
            Optional.empty(),
            DeltaStepping,
            DimensionTransformer.DISABLED,
            estimationFacade::deltaStepping,
            (graph, __) -> pathFindingAlgorithms.deltaStepping(graph, configuration),
            Optional.of(sideEffect),
            new MutateResultRenderer<>(configuration, resultBuilder)
        );
    }


    public <RESULT> RESULT spanningTree(
        GraphName graphName,
        SpanningTreeMutateConfig configuration,
        ResultBuilder<SpanningTreeMutateConfig, SpanningTree, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new SpanningTreeMutateStep(
            configuration.mutateRelationshipType(),
            configuration.mutateProperty(),
            mutateRelationshipService
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            AlgorithmLabel.SpanningTree,
            estimationFacade::spanningTree,
            (graph, __) -> pathFindingAlgorithms.spanningTree(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT steinerTree(
        GraphName graphName,
        SteinerTreeMutateConfig configuration,
        ResultBuilder<SteinerTreeMutateConfig, SteinerTreeResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new SteinerTreeMutateStep(
            configuration.mutateRelationshipType(),
            configuration.mutateProperty(),
            configuration.sourceNode(),
            mutateRelationshipService
        );

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateMode(
            graphName,
            configuration,
            SteinerTree,
            () -> estimationFacade.steinerTree(configuration),
            (graph, __) -> pathFindingAlgorithms.steinerTree(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }
}
