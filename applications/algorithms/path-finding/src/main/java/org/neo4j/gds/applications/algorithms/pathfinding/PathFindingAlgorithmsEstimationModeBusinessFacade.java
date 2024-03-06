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

import org.neo4j.gds.AlgorithmMemoryEstimateDefinition;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.paths.astar.AStarMemoryEstimateDefinition;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraMemoryEstimateDefinition;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.yens.YensMemoryEstimateDefinition;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.gds.results.MemoryEstimateResult;

/**
 * Here is the top level business facade for all your path finding memory estimation needs.
 * It will have all pathfinding algorithms on it, in estimate mode.
 */
public class PathFindingAlgorithmsEstimationModeBusinessFacade {
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;

    public PathFindingAlgorithmsEstimationModeBusinessFacade(AlgorithmEstimationTemplate algorithmEstimationTemplate) {
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
    }

    public MemoryEstimateResult singlePairShortestPathAStarEstimate(
        ShortestPathAStarBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        return runEstimation(new AStarMemoryEstimateDefinition(), configuration,  graphNameOrConfiguration);
    }

    public MemoryEstimateResult singlePairShortestPathDijkstraEstimate(
        DijkstraSourceTargetsBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        return runEstimation(new DijkstraMemoryEstimateDefinition(configuration.toMemoryEstimateParameters()), configuration, graphNameOrConfiguration);
    }

    public MemoryEstimateResult singlePairShortestPathYensEstimate(
        ShortestPathYensBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        return runEstimation(new YensMemoryEstimateDefinition(configuration.k()), configuration, graphNameOrConfiguration);
    }

    public MemoryEstimateResult singleSourceShortestPathDijkstraEstimate(
        DijkstraBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        return runEstimation(new DijkstraMemoryEstimateDefinition(configuration.toMemoryEstimateParameters()), configuration, graphNameOrConfiguration);
    }

    // TODO: remove this fella once finished with the estimate definitions
    private <CONFIGURATION extends AlgoBaseConfig> MemoryEstimateResult runEstimation(
        AlgorithmMemoryEstimateDefinition memoryEstimateDefinition,
        CONFIGURATION configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = memoryEstimateDefinition.memoryEstimation();

        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }

}
