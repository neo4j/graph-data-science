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
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.config.RelationshipWeightConfig;
import org.neo4j.gds.config.WriteRelationshipConfig;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.progress.TaskRegistryFactory;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.paths.WritePathOptionsConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.A_STAR;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.DIJKSTRA;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.YENS;

/**
 * Here is the top level business facade for all your path finding write needs.
 * It will have all pathfinding algorithms on it, in write mode.
 */
public class PathFindingAlgorithmsWriteModeBusinessFacade {
    private final Log log;

    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;
    private final RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;

    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithms pathFindingAlgorithms;

    public PathFindingAlgorithmsWriteModeBusinessFacade(
        Log log,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithms pathFindingAlgorithms
    ) {
        this.log = log;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.relationshipStreamExporterBuilder = relationshipStreamExporterBuilder;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationFlag = terminationFlag;
        this.estimationFacade = estimationFacade;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
    }

    public <RESULT> RESULT singlePairShortestPathAStarWrite(
        GraphName graphName,
        ShortestPathAStarWriteConfig configuration,
        ResultBuilder<ShortestPathAStarWriteConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            A_STAR,
            () -> estimationFacade.singlePairShortestPathAStarEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathDijkstraWrite(
        GraphName graphName,
        ShortestPathDijkstraWriteConfig configuration,
        ResultBuilder<ShortestPathDijkstraWriteConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            DIJKSTRA,
            () -> estimationFacade.singlePairShortestPathDijkstraEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathYensWrite(
        GraphName graphName,
        ShortestPathYensWriteConfig configuration,
        ResultBuilder<ShortestPathYensWriteConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            YENS,
            () -> estimationFacade.singlePairShortestPathYensEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathYens(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singleSourceShortestPathDijkstraWrite(
        GraphName graphName,
        AllShortestPathsDijkstraWriteConfig configuration,
        ResultBuilder<AllShortestPathsDijkstraWriteConfig, PathFindingResult, RESULT> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            DIJKSTRA,
            () -> estimationFacade.singleSourceShortestPathDijkstraEstimation(configuration),
            graph -> pathFindingAlgorithms.singleSourceShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    private <CONFIGURATION extends AlgoBaseConfig & RelationshipWeightConfig & WriteRelationshipConfig & WritePathOptionsConfig, RESULT> RESULT runAlgorithmAndWrite(
        GraphName graphName,
        CONFIGURATION configuration,
        String label,
        Supplier<MemoryEstimation> memoryEstimation,
        AlgorithmComputation<PathFindingResult> algorithm,
        ResultBuilder<CONFIGURATION, PathFindingResult, RESULT> resultBuilder
    ) {
        MutateOrWriteStep<CONFIGURATION, PathFindingResult> writeStep = new ShortestPathWriteStep<>(
            log,
            relationshipStreamExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            configuration
        );

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
