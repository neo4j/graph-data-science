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
import org.neo4j.gds.paths.astar.AStarMemoryEstimateDefinition;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarMutateConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraMemoryEstimateDefinition;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.paths.yens.YensMemoryEstimateDefinition;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;

import java.util.Optional;

import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.A_STAR;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.DIJKSTRA;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.YENS;

/**
 * Here is the top level business facade for all your path finding mutate needs.
 * It will have all pathfinding algorithms on it, in mutate mode.
 */
public class PathFindingAlgorithmsMutateModeBusinessFacade {
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;

    private final PathFindingAlgorithms pathFindingAlgorithms;

    public PathFindingAlgorithmsMutateModeBusinessFacade(
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        PathFindingAlgorithms pathFindingAlgorithms
    ) {
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
    }

    public <RESULT> RESULT singlePairShortestPathAStarMutate(
        GraphName graphName,
        ShortestPathAStarMutateConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep(configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            A_STAR,
            () -> new AStarMemoryEstimateDefinition().memoryEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathDijkstraMutate(
        GraphName graphName,
        ShortestPathDijkstraMutateConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep(configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            DIJKSTRA,
            () -> new DijkstraMemoryEstimateDefinition().memoryEstimation(configuration.toMemoryEstimateParameters()),
            graph -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathYensMutate(
        GraphName graphName,
        ShortestPathYensMutateConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        var mutateStep = new ShortestPathMutateStep(configuration);

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            YENS,
            () -> new YensMemoryEstimateDefinition().memoryEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathYens(graph, configuration),
            Optional.of(mutateStep),
            resultBuilder
        );
    }

    public <RESULT> RESULT singleSourceShortestPathDijkstraMutate(
        GraphName graphName,
        AllShortestPathsDijkstraMutateConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            DIJKSTRA,
            () -> new DijkstraMemoryEstimateDefinition().memoryEstimation(configuration.toMemoryEstimateParameters()),
            graph -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            Optional.of(new ShortestPathMutateStep(configuration)),
            resultBuilder
        );
    }
}
