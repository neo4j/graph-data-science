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
package org.neo4j.gds.procedures.algorithms.pathfinding.stats;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.maxflow.MaxFlowStatsConfig;
import org.neo4j.gds.mcmf.MCMFStatsConfig;
import org.neo4j.gds.pathfinding.PathFindingComputeBusinessFacade;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordStatsConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaStatsConfig;
import org.neo4j.gds.paths.traverse.BfsStatsConfig;
import org.neo4j.gds.pcst.PCSTStatsConfig;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.BellmanFordStatsResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.MCMFStatsResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.MaxFlowStatsResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.PrizeCollectingSteinerTreeStatsResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeStatsResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.SteinerStatsResult;
import org.neo4j.gds.procedures.algorithms.results.StandardModeResult;
import org.neo4j.gds.procedures.algorithms.results.StandardStatsResult;
import org.neo4j.gds.spanningtree.SpanningTreeStatsConfig;
import org.neo4j.gds.steiner.SteinerTreeStatsConfig;
import org.neo4j.gds.traversal.RandomWalkStatsConfig;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackPathFindingStatsProcedureFacade {
    private final PathFindingComputeBusinessFacade businessFacade;

    private final UserSpecificConfigurationParser configurationParser;

    public PushbackPathFindingStatsProcedureFacade(
        PathFindingComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
    }


    public Stream<BellmanFordStatsResult> bellmanFord(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsBellmanFordStatsConfig::of
        );

        return businessFacade.bellmanFord(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new BellmanFordStatsResultTransformerBuilder(config)
        ).join();
    }

    public Stream<StandardStatsResult> breadthFirstSearch(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            BfsStatsConfig::of
        );

        return businessFacade.breadthFirstSearch(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new TraversalStatsResultTransformerBuilder(config::toMap)
        ).join();
    }

    public Stream<StandardStatsResult> deltaStepping(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsDeltaStatsConfig::of
        );

        return businessFacade.deltaStepping(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new DeltaSteppingStatsResultTransformerBuilder(config)
        ).join();
    }

    public Stream<MaxFlowStatsResult> maxFlow(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            MaxFlowStatsConfig::of
        );

        return businessFacade.maxFlow(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new MaxFlowStatsResultTransformerBuilder(config)
        ).join();
    }

    public Stream<MCMFStatsResult> mcmf(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            MCMFStatsConfig::of
        );

        return businessFacade.mcmf(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.costProperty(),
            config.toMCMFParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new MCMFStatsResultTransformer(configuration)
        ).join();
    }


    public Stream<PrizeCollectingSteinerTreeStatsResult> prizeCollectingSteinerTree(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            PCSTStatsConfig::of
        );

        return businessFacade.pcst(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new PrizeCollectingSteinerTreeStatsResultTransformerBuilder(config)
        ).join();
    }

    public Stream<StandardModeResult> randomWalk(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            RandomWalkStatsConfig::of
        );

        return businessFacade.randomWalk(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new StandardModeStatsResultTransformerBuilder<>(config::toMap)
        ).join();
    }

    public Stream<SpanningTreeStatsResult> spanningTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SpanningTreeStatsConfig::of
        );

        return businessFacade.spanningTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new SpanningTreeStatsResultTransformerBuilder(config)
        ).join();
    }

    public Stream<SteinerStatsResult> steinerTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SteinerTreeStatsConfig::of
        );

        return businessFacade.steinerTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new SteinerTreeStatsResultTransformerBuilder(config)
        ).join();

    }

}
