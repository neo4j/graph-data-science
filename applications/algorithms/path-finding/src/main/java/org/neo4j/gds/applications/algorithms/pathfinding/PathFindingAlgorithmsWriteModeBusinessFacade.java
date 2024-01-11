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
import org.neo4j.gds.paths.astar.AStarMemoryEstimateDefinition;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraMemoryEstimateDefinition;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.yens.YensMemoryEstimateDefinition;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.Optional;
import java.util.function.Supplier;

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
public class PathFindingAlgorithmsWriteModeBusinessFacade {
    private final Log log;

    private final AlgorithmProcessingTemplate algorithmProcessingTemplate;
    private final RelationshipStreamExporterBuilder relationshipStreamExporterBuilder;
    private final TaskRegistryFactory taskRegistryFactory;
    private final TerminationFlag terminationFlag;

    private final PathFindingAlgorithms pathFindingAlgorithms;

    public PathFindingAlgorithmsWriteModeBusinessFacade(
        Log log,
        AlgorithmProcessingTemplate algorithmProcessingTemplate,
        RelationshipStreamExporterBuilder relationshipStreamExporterBuilder,
        TaskRegistryFactory taskRegistryFactory,
        TerminationFlag terminationFlag,
        PathFindingAlgorithms pathFindingAlgorithms
    ) {
        this.log = log;
        this.algorithmProcessingTemplate = algorithmProcessingTemplate;
        this.relationshipStreamExporterBuilder = relationshipStreamExporterBuilder;
        this.terminationFlag = terminationFlag;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
        this.taskRegistryFactory = taskRegistryFactory;
    }

    public <RESULT> RESULT singlePairShortestPathAStarWrite(
        GraphName graphName,
        ShortestPathAStarWriteConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            "AStar",
            () -> new AStarMemoryEstimateDefinition().memoryEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathAStar(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathDijkstraWrite(
        GraphName graphName,
        ShortestPathDijkstraWriteConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            "Dijkstra",
            () -> new DijkstraMemoryEstimateDefinition().memoryEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathDijkstra(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT singlePairShortestPathYensWrite(
        GraphName graphName,
        ShortestPathYensWriteConfig configuration,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return runAlgorithmAndWrite(
            graphName,
            configuration,
            "Yens",
            () -> new YensMemoryEstimateDefinition().memoryEstimation(configuration),
            graph -> pathFindingAlgorithms.singlePairShortestPathYens(graph, configuration),
            resultBuilder
        );
    }

    private <CONFIGURATION extends AlgoBaseConfig & RelationshipWeightConfig & WriteRelationshipConfig & WritePathOptionsConfig, RESULT> RESULT runAlgorithmAndWrite(
        GraphName graphName,
        CONFIGURATION configuration,
        String label,
        Supplier<MemoryEstimation> memoryEstimation,
        AlgorithmComputation<PathFindingResult> algorithm,
        ResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        Optional<MutateOrWriteStep<PathFindingResult>> writeStep = Optional.of(new ShortestPathWriteStep<>(
            log,
            relationshipStreamExporterBuilder,
            taskRegistryFactory,
            terminationFlag,
            configuration
        ));

        return algorithmProcessingTemplate.processAlgorithm(
            graphName,
            configuration,
            label,
            memoryEstimation,
            algorithm,
            writeStep,
            resultBuilder
        );
    }
}
