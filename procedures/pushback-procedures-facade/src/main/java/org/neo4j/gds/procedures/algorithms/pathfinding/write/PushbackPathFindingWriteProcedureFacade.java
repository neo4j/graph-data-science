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
package org.neo4j.gds.procedures.algorithms.pathfinding.write;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.pathfinding.PathFindingComputeBusinessFacade;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordWriteConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaWriteConfig;
import org.neo4j.gds.pcst.PCSTWriteConfig;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.BellmanFordWriteResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.KSpanningTreeWriteResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.PrizeCollectingSteinerTreeWriteResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeWriteResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.SteinerWriteResult;
import org.neo4j.gds.procedures.algorithms.results.StandardWriteRelationshipsResult;
import org.neo4j.gds.spanningtree.SpanningTreeWriteConfig;
import org.neo4j.gds.steiner.SteinerTreeWriteConfig;

import java.util.Map;
import java.util.stream.Stream;

public class PushbackPathFindingWriteProcedureFacade {
    private final PathFindingComputeBusinessFacade businessFacade;

    private final UserSpecificConfigurationParser configurationParser;

    public PushbackPathFindingWriteProcedureFacade(
        PathFindingComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
    }

    public Stream<BellmanFordWriteResult> bellmanFord(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsBellmanFordWriteConfig::of
        );

        return businessFacade.bellmanFord(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new BellmanFordWriteResultTransformerBuilder(config)
        ).join();
    }

    public Stream<StandardWriteRelationshipsResult> deltaStepping(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsDeltaWriteConfig::of
        );

        return businessFacade.deltaStepping(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new DeltaSteppingWriteResultTransformerBuilder(config)
        ).join();
    }

    public Stream<KSpanningTreeWriteResult> kSpanningTree(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }


    public Stream<PrizeCollectingSteinerTreeWriteResult> pcst(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            PCSTWriteConfig::of
        );

        return businessFacade.pcst(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new PCSTWriteResultTransformerBuilder(config)
        ).join();
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathAStar(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathDijkstra(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathYens(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    public Stream<StandardWriteRelationshipsResult> singleSourceShortestPathDijkstra(
        String graphName,
        Map<String, Object> configuration
    ) {
        return Stream.empty();
    }

    public Stream<SpanningTreeWriteResult> spanningTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SpanningTreeWriteConfig::of
        );

        return businessFacade.spanningTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new SpanningTreeWriteResultTransformerBuilder(config)
        ).join();
    }

    public Stream<SteinerWriteResult> steinerTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SteinerTreeWriteConfig::of
        );

        return businessFacade.steinerTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new SteinerTreeWriteResultTransformerBuilder(config)
        ).join();

    }

}
