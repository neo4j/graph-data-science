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
import org.neo4j.gds.core.write.RelationshipExporterBuilder;
import org.neo4j.gds.core.write.RelationshipStreamExporterBuilder;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.paths.WritePathOptionsConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.function.Supplier;

import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.A_STAR;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.DIJKSTRA;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.STEINER;
import static org.neo4j.gds.applications.algorithms.pathfinding.AlgorithmLabels.YENS;

/**
 * Here is the top level business facade for all your path finding write needs.
 * It will have all pathfinding algorithms on it, in write mode.
 */
public class PathFindingAlgorithmsWriteModeBusinessFacade {
    private final Log log;

    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;
    private final RelationshipExporterBuilder relationshipExporterBuilder;
    private final RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;

    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithms pathFindingAlgorithms;

    public PathFindingAlgorithmsWriteModeBusinessFacade(
        Log log,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        RelationshipExporterBuilder relationshipExporterBuilder,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithms pathFindingAlgorithms
    ) {
        this.log = log;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.relationshipExporterBuilder = relationshipExporterBuilder;
        this.relationshipStreamExporterBuilder = relationshipStreamExporterBuilder;
        this.taskRegistryFactory = taskRegistryFactory;
        this.terminationFlag = terminationFlag;
        this.estimationFacade = estimationFacade;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
    }

    public <RESULT> RESULT singlePairShortestPathAStar(
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

    public <RESULT> RESULT singlePairShortestPathDijkstra(
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

    public <RESULT> RESULT singlePairShortestPathYens(
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

    public <RESULT> RESULT singleSourceShortestPathDijkstra(
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

    public <RESULT> RESULT steinerTree(
        GraphName graphName,
        SteinerTreeWriteConfig configuration,
        ResultBuilder<SteinerTreeWriteConfig, SteinerTreeResult, RESULT> resultBuilder
    ) {
        var writeStep = new SteinerTreeWriteStep(relationshipExporterBuilder, terminationFlag, configuration);

        return runAlgorithmAndWrite(
            graphName,
            configuration,
            STEINER,
            () -> estimationFacade.steinerTreeEstimation(configuration),
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
        String label,
        Supplier<MemoryEstimation> memoryEstimation,
        AlgorithmComputation<PathFindingResult> algorithm,
        ResultBuilder<CONFIGURATION, PathFindingResult, RESULT_TO_CALLER> resultBuilder
    ) {
        var writeStep = new ShortestPathWriteStep<>(
            log,
            relationshipStreamExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
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

    private <CONFIGURATION extends AlgoBaseConfig & RelationshipWeightConfig & WriteRelationshipConfig, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> RESULT_TO_CALLER runAlgorithmAndWrite(
        GraphName graphName,
        CONFIGURATION configuration,
        String label,
        Supplier<MemoryEstimation> memoryEstimation,
        AlgorithmComputation<RESULT_FROM_ALGORITHM> algorithm,
        MutateOrWriteStep<RESULT_FROM_ALGORITHM> writeStep,
        ResultBuilder<CONFIGURATION, RESULT_FROM_ALGORITHM, RESULT_TO_CALLER> resultBuilder
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
