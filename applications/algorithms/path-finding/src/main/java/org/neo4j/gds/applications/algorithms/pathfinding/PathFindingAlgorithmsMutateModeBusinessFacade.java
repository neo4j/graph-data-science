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

import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodeProperty;
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.Algorithm;
import org.neo4j.gds.applications.algorithms.metadata.NodePropertiesWritten;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
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
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkMutateConfig;

import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.AStar;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.BFS;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.BellmanFord;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.DFS;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.DeltaStepping;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Dijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.RandomWalk;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.SingleSourceDijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.SteinerTree;
import static org.neo4j.gds.applications.algorithms.metadata.Algorithm.Yens;

/**
 * Here is the top level business facade for all your path finding mutate needs.
 * It will have all pathfinding algorithms on it, in mutate mode.
 */
public class PathFindingAlgorithmsMutateModeBusinessFacade {
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithms pathFindingAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;
    private final MutateNodeProperty mutateNodeProperty;

    public PathFindingAlgorithmsMutateModeBusinessFacade(
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithms pathFindingAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        MutateNodeProperty mutateNodeProperty
    ) {
        this.pathFindingAlgorithms = pathFindingAlgorithms;
        this.estimationFacade = estimationFacade;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.mutateNodeProperty = mutateNodeProperty;
    }

    public <RESULT> RESULT bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordMutateConfig configuration,
        ResultBuilder<AllShortestPathsBellmanFordMutateConfig, BellmanFordResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new BellmanFordMutateStep(configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
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
        var mutateRelationshipType = RelationshipType.of(configuration.mutateRelationshipType());
        var mutateStep = new SearchMutateStep(mutateRelationshipType);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
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
        var mutateStep = new ShortestPathMutateStep(configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
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
        var mutateRelationshipType = RelationshipType.of(configuration.mutateRelationshipType());
        var mutateStep = new SearchMutateStep(mutateRelationshipType);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            DFS,
            estimationFacade::depthFirstSearch,
            (graph, __) -> pathFindingAlgorithms.depthFirstSearch(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT randomWalk(
        GraphName graphName,
        RandomWalkMutateConfig configuration,
        ResultBuilder<RandomWalkMutateConfig, HugeAtomicLongArray, RESULT, NodePropertiesWritten> resultBuilder
    ) {
        var mutateStep = new RandomWalkCountingNodeVisitsMutateStep(mutateNodeProperty, configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
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
        var mutateStep = new ShortestPathMutateStep(configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
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
        var mutateStep = new ShortestPathMutateStep(configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            Dijkstra,
            () -> estimationFacade.singlePairShortestPathDijkstra(configuration),
            (graph, __) -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathYens(
        GraphName graphName,
        ShortestPathYensMutateConfig configuration,
        ResultBuilder<ShortestPathYensMutateConfig, PathFindingResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep(configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
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
        var mutateStep = new ShortestPathMutateStep(configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            SingleSourceDijkstra,
            () -> estimationFacade.singleSourceShortestPathDijkstra(configuration),
            (graph, __) -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }

    public <RESULT> RESULT spanningTree(
        GraphName graphName,
        SpanningTreeMutateConfig configuration,
        ResultBuilder<SpanningTreeMutateConfig, SpanningTree, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new SpanningTreeMutateStep(configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            Algorithm.SpanningTree,
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
        var mutateStep = new SteinerTreeMutateStep(configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
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
