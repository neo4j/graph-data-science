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
import org.neo4j.gds.applications.algorithms.machinery.ResultBuilder;
import org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking;
import org.neo4j.gds.applications.algorithms.metadata.RelationshipsWritten;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarMutateConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordMutateConfig;
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

import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.AStar;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BFS;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.BellmanFord;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.DFS;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.DeltaStepping;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Dijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.SingleSourceDijkstra;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.SteinerTree;
import static org.neo4j.gds.applications.algorithms.metadata.LabelForProgressTracking.Yens;

/**
 * Here is the top level business facade for all your path finding mutate needs.
 * It will have all pathfinding algorithms on it, in mutate mode.
 */
public class PathFindingAlgorithmsMutateModeBusinessFacade {
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithms pathFindingAlgorithms;
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    public PathFindingAlgorithmsMutateModeBusinessFacade(
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithms pathFindingAlgorithms,
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience
    ) {
        this.pathFindingAlgorithms = pathFindingAlgorithms;
        this.estimationFacade = estimationFacade;
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
    }

    public <RESULT> RESULT bellmanFord(
        GraphName graphName,
        BellmanFordMutateConfig configuration,
        ResultBuilder<BellmanFordMutateConfig, BellmanFordResult, RESULT, RelationshipsWritten> resultBuilder
    ) {
        var mutateStep = new BellmanFordMutateStep(configuration);

        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInMutateOrWriteMode(
            graphName,
            configuration,
            BellmanFord,
            () -> estimationFacade.bellmanFord(configuration),
            graph -> pathFindingAlgorithms.bellmanFord(graph, configuration),
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
            graph -> pathFindingAlgorithms.breadthFirstSearch(graph, configuration),
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
            graph -> pathFindingAlgorithms.deltaStepping(graph, configuration),
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
            graph -> pathFindingAlgorithms.depthFirstSearch(graph, configuration),
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
            graph -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
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
            graph -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
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
            graph -> pathFindingAlgorithms.singlePairShortestPathYens(graph, configuration),
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
            graph -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
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
            LabelForProgressTracking.SpanningTree,
            estimationFacade::spanningTree,
            graph -> pathFindingAlgorithms.spanningTree(graph, configuration),
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
            graph -> pathFindingAlgorithms.steinerTree(graph, configuration),
            mutateStep,
            resultBuilder
        );
    }
}
