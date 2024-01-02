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
import org.neo4j.gds.paths.astar.config.ShortestPathAStarStreamConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraMemoryEstimateDefinition;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfig;

import java.util.Optional;

/**
 * Here is the top level business facade for all your path finding needs.
 * It will have all pathfinding algorithms on it, each in four modes {mutate, stats, stream, write},
 * and algorithm estimates too.
 * <p>
 * That could get really long so consider something cleverer, like per mode sub-facades, or algorithm sub-facades.
 * Prolly the former because cohesion.
 * <p>
 * Because this is the top level thing, we should make it very useful and usable,
 * in the sense that we can capture reuse here that different UIs need. Neo4j Procedures coming in, maybe Arrow,
 * let's make life easy for them.
 * <p>
 * Concretely, we have UI layers inject result rendering as that is bespoke.
 * We delegate downwards for the actual computations.
 * But importantly, this is where we decide which, if any, mutate or write hooks need to be injected.
 */
public class PathFindingAlgorithmsFacade {
    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;
    private final PathFindingAlgorithms pathFindingAlgorithms;

    public PathFindingAlgorithmsFacade(
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        PathFindingAlgorithms pathFindingAlgorithms
    ) {
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
    }

    public <RESULT> RESULT singlePairShortestPathAStarStream(
        GraphName graphName,
        ShortestPathAStarStreamConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            "AStar",
            () -> new AStarMemoryEstimateDefinition().memoryEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathDijkstraStream(
        GraphName graphName,
        ShortestPathDijkstraStreamConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            "Dijkstra",
            () -> new DijkstraMemoryEstimateDefinition().memoryEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            Optional.empty(),
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
            "Dijkstra",
            () -> new DijkstraMemoryEstimateDefinition().memoryEstimation(configuration),
            graph -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            Optional.of(new ShortestPathMutateStep(configuration)),
            resultBuilder
        );
    }

    public <RESULT> RESULT singleSourceShortestPathDijkstraStream(
        GraphName graphName,
        AllShortestPathsDijkstraStreamConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            "Dijkstra",
            () -> new DijkstraMemoryEstimateDefinition().memoryEstimation(configuration),
            graph -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            Optional.empty(),
            resultBuilder
        );
    }
}
