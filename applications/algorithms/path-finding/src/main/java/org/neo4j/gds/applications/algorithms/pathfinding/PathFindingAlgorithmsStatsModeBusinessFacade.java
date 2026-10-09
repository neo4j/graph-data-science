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
import org.neo4j.gds.applications.algorithms.execution.machinery.Synchroniser;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultBuilder;
import org.neo4j.gds.applications.algorithms.machinery.StatsResultRenderer;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowBaseConfig;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.mcmf.MCMFBaseConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.pcst.PCSTBaseConfig;
import org.neo4j.gds.pricesteiner.PrizeCollectingSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

import java.util.Optional;
import java.util.stream.Stream;

public class PathFindingAlgorithmsStatsModeBusinessFacade {
    private final InstrumentedPathFindingAlgorithms instrumentedPathFindingAlgorithms;
    private final Synchroniser synchroniser;

    PathFindingAlgorithmsStatsModeBusinessFacade(
        InstrumentedPathFindingAlgorithms instrumentedPathFindingAlgorithms,
        Synchroniser synchroniser
    ) {
        this.instrumentedPathFindingAlgorithms = instrumentedPathFindingAlgorithms;
        this.synchroniser = synchroniser;
    }

    public <RESULT> RESULT bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordBaseConfig configuration,
        StatsResultBuilder<BellmanFordResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.bellmanFord(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT breadthFirstSearch(
        GraphName graphName,
        BfsBaseConfig configuration,
        StatsResultBuilder<HugeLongArray, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.breadthFirstSearch(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaBaseConfig configuration,
        StatsResultBuilder<PathFindingResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.deltaStepping(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT maxFlow(
        GraphName graphName,
        MaxFlowBaseConfig configuration,
        StatsResultBuilder<FlowResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.maxFlow(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT minCostMaxFlow(
        GraphName graphName,
        MCMFBaseConfig configuration,
        StatsResultBuilder<CostFlowResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.minCostMaxFlow(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT prizeCollectingSteinerTree(
        GraphName graphName,
        PCSTBaseConfig configuration,
        StatsResultBuilder<PrizeCollectingSteinerTreeResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.prizeCollectingSteinerTree(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT randomWalk(
        GraphName graphName,
        RandomWalkBaseConfig configuration,
        StatsResultBuilder<Stream<long[]>, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.randomWalk(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT spanningTree(
        GraphName graphName,
        SpanningTreeBaseConfig configuration,
        StatsResultBuilder<SpanningTree, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.spanningTree(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }

    public <RESULT> RESULT steinerTree(
        GraphName graphName,
        SteinerTreeBaseConfig configuration,
        StatsResultBuilder<SteinerTreeResult, RESULT> resultBuilder
    ) {
        return synchroniser.synchronise(() -> instrumentedPathFindingAlgorithms.steinerTree(
            graphName,
            configuration,
            Optional.empty(),
            new StatsResultRenderer<>(resultBuilder)
        ));
    }
}
