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
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmProcessingTemplateConvenience;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordStatsConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaStatsConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.traverse.BfsStatsConfig;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeStatsConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.steiner.SteinerTreeStatsConfig;
import org.neo4j.gds.traversal.RandomWalkStatsConfig;

import java.util.stream.Stream;

import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BFS;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.BellmanFord;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.DeltaStepping;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.RandomWalk;
import static org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel.SteinerTree;

public class PathFindingAlgorithmsStatsModeBusinessFacade {
    private final AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience;

    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final PathFindingAlgorithms pathFindingAlgorithms;

    PathFindingAlgorithmsStatsModeBusinessFacade(
        AlgorithmProcessingTemplateConvenience algorithmProcessingTemplateConvenience,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        PathFindingAlgorithms pathFindingAlgorithms
    ) {
        this.algorithmProcessingTemplateConvenience = algorithmProcessingTemplateConvenience;
        this.estimationFacade = estimationFacade;
        this.pathFindingAlgorithms = pathFindingAlgorithms;
    }

    public <RESULT> RESULT bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordStatsConfig configuration,
        StatsResultBuilder<BellmanFordResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            BellmanFord,
            () -> estimationFacade.bellmanFord(configuration),
            (graph, __) -> pathFindingAlgorithms.bellmanFord(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT breadthFirstSearch(
        GraphName graphName,
        BfsStatsConfig configuration,
        StatsResultBuilder<HugeLongArray, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            BFS,
            estimationFacade::breadthFirstSearch,
            (graph, __) -> pathFindingAlgorithms.breadthFirstSearch(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaStatsConfig configuration,
        StatsResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            DeltaStepping,
            estimationFacade::deltaStepping,
            (graph, __) -> pathFindingAlgorithms.deltaStepping(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT randomWalk(
        GraphName graphName,
        RandomWalkStatsConfig configuration,
        StatsResultBuilder<Stream<long[]>, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            RandomWalk,
            () -> estimationFacade.randomWalk(configuration),
            (graph, __) -> pathFindingAlgorithms.randomWalk(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT spanningTree(
        GraphName graphName,
        SpanningTreeStatsConfig configuration,
        StatsResultBuilder<SpanningTree, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            AlgorithmLabel.SpanningTree,
            estimationFacade::spanningTree,
            (graph, __) -> pathFindingAlgorithms.spanningTree(graph, configuration),
            resultBuilder
        );
    }

    public <RESULT> RESULT steinerTree(
        GraphName graphName,
        SteinerTreeStatsConfig configuration,
        StatsResultBuilder<SteinerTreeResult, RESULT> resultBuilder
    ) {
        return algorithmProcessingTemplateConvenience.processRegularAlgorithmInStatsMode(
            graphName,
            configuration,
            SteinerTree,
            () -> estimationFacade.steinerTree(configuration),
            (graph, __) -> pathFindingAlgorithms.steinerTree(graph, configuration),
            resultBuilder
        );
    }
}
