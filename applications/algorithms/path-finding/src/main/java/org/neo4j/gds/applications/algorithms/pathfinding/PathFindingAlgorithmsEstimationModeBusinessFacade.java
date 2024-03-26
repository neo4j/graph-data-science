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

import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.kspanningtree.KSpanningTreeWriteConfig;
import org.neo4j.gds.paths.astar.AStarMemoryEstimateDefinition;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraMemoryEstimateDefinition;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsMemoryEstimateDefinition;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.paths.traverse.DfsMemoryEstimateDefinition;
import org.neo4j.gds.paths.yens.YensMemoryEstimateDefinition;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.gds.results.MemoryEstimateResult;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.spanningtree.SpanningTreeMemoryEstimateDefinition;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeMemoryEstimateDefinition;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;
import org.neo4j.gds.traversal.RandomWalkMemoryEstimateDefinition;

/**
 * Here is the top level business facade for all your path finding memory estimation needs.
 * It will have all pathfinding algorithms on it, in estimate mode.
 */
public class PathFindingAlgorithmsEstimationModeBusinessFacade {
    private final AlgorithmEstimationTemplate algorithmEstimationTemplate;

    public PathFindingAlgorithmsEstimationModeBusinessFacade(AlgorithmEstimationTemplate algorithmEstimationTemplate) {
        this.algorithmEstimationTemplate = algorithmEstimationTemplate;
    }

    public MemoryEstimateResult breadthFirstSearch(
        BfsBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = breadthFirstSearchEstimation(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation breadthFirstSearchEstimation(BfsBaseConfig ignored) {
        return new BfsMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult depthFirstSearch(
        DfsBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = depthFirstSearchEstimation(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation depthFirstSearchEstimation(DfsBaseConfig ignored) {
        return new DfsMemoryEstimateDefinition().memoryEstimation();
    }

    MemoryEstimation kSpanningTreeEstimation(KSpanningTreeWriteConfig ignored) {
        throw new MemoryEstimationNotImplementedException();
    }

    public MemoryEstimateResult randomWalk(
        RandomWalkBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = randomWalkEstimation(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    MemoryEstimation randomWalkEstimation(RandomWalkBaseConfig configuration) {
        return new RandomWalkMemoryEstimateDefinition(configuration.toMemoryEstimateParameters()).memoryEstimation();
    }

    public MemoryEstimateResult singlePairShortestPathAStar(
        ShortestPathAStarBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = singlePairShortestPathAStarEstimation(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation singlePairShortestPathAStarEstimation(ShortestPathAStarBaseConfig ignored) {
        return new AStarMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult singlePairShortestPathDijkstra(
        DijkstraSourceTargetsBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = singlePairShortestPathDijkstraEstimation(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation singlePairShortestPathDijkstraEstimation(DijkstraBaseConfig dijkstraBaseConfig) {
        var memoryEstimateParameters = dijkstraBaseConfig.toMemoryEstimateParameters();

        var memoryEstimateDefinition = new DijkstraMemoryEstimateDefinition(memoryEstimateParameters);

        return memoryEstimateDefinition.memoryEstimation();
    }

    public MemoryEstimateResult singlePairShortestPathYens(
        ShortestPathYensBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = singlePairShortestPathYensEstimation(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation singlePairShortestPathYensEstimation(ShortestPathYensBaseConfig configuration) {
        var memoryEstimateDefinition = new YensMemoryEstimateDefinition(configuration.k());

        return memoryEstimateDefinition.memoryEstimation();
    }

    public MemoryEstimateResult singleSourceShortestPathDijkstra(
        DijkstraBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = singleSourceShortestPathDijkstraEstimation(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation singleSourceShortestPathDijkstraEstimation(DijkstraBaseConfig configuration) {
        var memoryEstimateDefinition = new DijkstraMemoryEstimateDefinition(configuration.toMemoryEstimateParameters());

        return memoryEstimateDefinition.memoryEstimation();
    }

    public MemoryEstimateResult spanningTree(
        SpanningTreeBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = spanningTreeEstimation(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation spanningTreeEstimation(SpanningTreeBaseConfig ignored) {
        return new SpanningTreeMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult steinerTree(
        SteinerTreeBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = steinerTreeEstimation(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation steinerTreeEstimation(SteinerTreeBaseConfig configuration) {
        return new SteinerTreeMemoryEstimateDefinition(configuration.applyRerouting()).memoryEstimation();
    }

    public <CONFIGURATION extends AlgoBaseConfig> MemoryEstimateResult runEstimation(
        CONFIGURATION configuration,
        Object graphNameOrConfiguration,
        MemoryEstimation memoryEstimation
    ) {
        return algorithmEstimationTemplate.estimate(
            configuration,
            graphNameOrConfiguration,
            memoryEstimation
        );
    }
}
