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

import org.neo4j.gds.applications.algorithms.machinery.AlgorithmEstimationTemplate;
import org.neo4j.gds.applications.algorithms.machinery.MemoryEstimateResult;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.exceptions.MemoryEstimationNotImplementedException;
import org.neo4j.gds.mem.MemoryEstimation;
import org.neo4j.gds.paths.astar.AStarMemoryEstimateDefinition;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordMemoryEstimateDefinition;
import org.neo4j.gds.paths.delta.DeltaSteppingMemoryEstimateDefinition;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.DijkstraMemoryEstimateDefinition;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsMemoryEstimateDefinition;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.paths.traverse.DfsMemoryEstimateDefinition;
import org.neo4j.gds.paths.yens.YensMemoryEstimateDefinition;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
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

    MemoryEstimation allShortestPaths() {
        throw new MemoryEstimationNotImplementedException();
    }

    public MemoryEstimateResult bellmanFord(AllShortestPathsBellmanFordBaseConfig configuration, Object graphNameOrConfiguration) {
        var memoryEstimation = bellmanFord(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation bellmanFord(AllShortestPathsBellmanFordBaseConfig configuration) {
        return new BellmanFordMemoryEstimateDefinition(configuration.trackNegativeCycles()).memoryEstimation();
    }

    public MemoryEstimateResult breadthFirstSearch(
        BfsBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = breadthFirstSearch();

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation breadthFirstSearch() {
        return new BfsMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult deltaStepping(
        AllShortestPathsDeltaBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = deltaStepping();

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation deltaStepping() {
        return new DeltaSteppingMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult depthFirstSearch(
        DfsBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = depthFirstSearch();

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation depthFirstSearch() {
        return new DfsMemoryEstimateDefinition().memoryEstimation();
    }

    MemoryEstimation kSpanningTree() {
        throw new MemoryEstimationNotImplementedException();
    }

    MemoryEstimation longestPath() {
        throw new MemoryEstimationNotImplementedException();
    }

    public MemoryEstimateResult randomWalk(
        RandomWalkBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = randomWalk(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    MemoryEstimation randomWalk(RandomWalkBaseConfig configuration) {
        return new RandomWalkMemoryEstimateDefinition(configuration.toMemoryEstimateParameters()).memoryEstimation();
    }

    public MemoryEstimateResult singlePairShortestPathAStar(
        ShortestPathAStarBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = singlePairShortestPathAStar();

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation singlePairShortestPathAStar() {
        return new AStarMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult singlePairShortestPathDijkstra(
        DijkstraSourceTargetsBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = singlePairShortestPathDijkstra(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation singlePairShortestPathDijkstra(DijkstraBaseConfig dijkstraBaseConfig) {
        var memoryEstimateParameters = dijkstraBaseConfig.toMemoryEstimateParameters();

        var memoryEstimateDefinition = new DijkstraMemoryEstimateDefinition(memoryEstimateParameters);

        return memoryEstimateDefinition.memoryEstimation();
    }

    public MemoryEstimateResult singlePairShortestPathYens(
        ShortestPathYensBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = singlePairShortestPathYens(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation singlePairShortestPathYens(ShortestPathYensBaseConfig configuration) {
        var memoryEstimateDefinition = new YensMemoryEstimateDefinition(configuration.k());

        return memoryEstimateDefinition.memoryEstimation();
    }

    public MemoryEstimateResult singleSourceShortestPathDijkstra(
        DijkstraBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = singleSourceShortestPathDijkstra(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation singleSourceShortestPathDijkstra(DijkstraBaseConfig configuration) {
        var memoryEstimateDefinition = new DijkstraMemoryEstimateDefinition(configuration.toMemoryEstimateParameters());

        return memoryEstimateDefinition.memoryEstimation();
    }

    public MemoryEstimateResult spanningTree(
        SpanningTreeBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = spanningTree();

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation spanningTree() {
        return new SpanningTreeMemoryEstimateDefinition().memoryEstimation();
    }

    public MemoryEstimateResult steinerTree(
        SteinerTreeBaseConfig configuration,
        Object graphNameOrConfiguration
    ) {
        var memoryEstimation = steinerTree(configuration);

        return runEstimation(configuration, graphNameOrConfiguration, memoryEstimation);
    }

    public MemoryEstimation steinerTree(SteinerTreeBaseConfig configuration) {
        return new SteinerTreeMemoryEstimateDefinition(configuration.applyRerouting()).memoryEstimation();
    }

    MemoryEstimation topologicalSort() {
        throw new MemoryEstimationNotImplementedException();
    }

    private <CONFIGURATION extends AlgoBaseConfig> MemoryEstimateResult runEstimation(
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
