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
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarMutateConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.paths.traverse.BfsMutateConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.A_STAR;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.BFS;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.DIJKSTRA;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.STEINER;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.YENS;

/**
 * Here is the top level business facade for all your path finding mutate needs.
 * It will have all pathfinding algorithms on it, in mutate mode.
 */
public class PathFindingAlgorithmsMutateModeBusinessFacade {
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithms pathFindingAlgorithms;
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;

    public PathFindingAlgorithmsMutateModeBusinessFacade(
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithms pathFindingAlgorithms,
        AlgorithmProcessingTemplate algorithmProcessingTemplate
    ) {
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
        this.estimationFacade = estimationFacade;
    }

    public <RESULT> RESULT breadthFirstSearchMutate(
        GraphName graphName,
        BfsMutateConfig configuration,
        ResultBuilder<BfsMutateConfig, HugeLongArray, RESULT> resultBuilder
    ) {
        var mutateStep = new BreadthFirstSearchMutateStep(configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            BFS,
            () -> estimationFacade.breadthFirstSearchEstimation(configuration),
            graph -> pathFindingAlgorithms.breadthFirstSearch(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathAStarMutate(
        GraphName graphName,
        ShortestPathAStarMutateConfig configuration,
        ResultBuilder<ShortestPathAStarMutateConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep<ShortestPathAStarMutateConfig>(configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            A_STAR,
            () -> estimationFacade.singlePairShortestPathAStarEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathDijkstraMutate(
        GraphName graphName,
        ShortestPathDijkstraMutateConfig configuration,
        ResultBuilder<ShortestPathDijkstraMutateConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep<ShortestPathDijkstraMutateConfig>(configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            DIJKSTRA,
            () -> estimationFacade.singlePairShortestPathDijkstraEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathYensMutate(
        GraphName graphName,
        ShortestPathYensMutateConfig configuration,
        ResultBuilder<ShortestPathYensMutateConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep<ShortestPathYensMutateConfig>(configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            YENS,
            () -> estimationFacade.singlePairShortestPathYensEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathYens(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT singleSourceShortestPathDijkstraMutate(
        GraphName graphName,
        AllShortestPathsDijkstraMutateConfig configuration,
        ResultBuilder<AllShortestPathsDijkstraMutateConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep<AllShortestPathsDijkstraMutateConfig>(configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            DIJKSTRA,
            () -> estimationFacade.singleSourceShortestPathDijkstraEstimation(configuration),
            graph -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT steinerTreeMutate(
        GraphName graphName,
        SteinerTreeMutateConfig configuration,
        ResultBuilder<SteinerTreeMutateConfig, SteinerTreeResult, RESULT> resultBuilder
    ) {
        var mutateOrWriteStep = new SteinerTreeMutateStep(configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            STEINER,
            () -> estimationFacade.steinerTreeEstimation(configuration),
            graph -> pathFindingAlgorithms.steinerTree(graph, configuration),
            Optional.of(mutateOrWriteStep),
            resultBuilder
        );
    }
}
