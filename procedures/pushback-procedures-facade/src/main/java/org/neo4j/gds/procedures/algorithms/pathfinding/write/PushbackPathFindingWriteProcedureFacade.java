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
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.WriteContext;
import org.neo4j.gds.applications.algorithms.machinery.WriteRelationshipService;
import org.neo4j.gds.kspanningtree.KSpanningTreeWriteConfig;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.pathfinding.BellmanFordWriteStep;
import org.neo4j.gds.pathfinding.KSpanningTreeWriteStep;
import org.neo4j.gds.pathfinding.PathFindingComputeBusinessFacade;
import org.neo4j.gds.pathfinding.PrizeCollectingSteinerTreeWriteStep;
import org.neo4j.gds.pathfinding.ShortestPathWriteStep;
import org.neo4j.gds.pathfinding.SpanningTreeWriteStep;
import org.neo4j.gds.pathfinding.SteinerTreeWriteStep;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarWriteConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordWriteConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraWriteConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraWriteConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensWriteConfig;
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

    private final RequestScopedDependencies requestScopedDependencies;
    private final WriteContext writeContext;

    private final Log log;

    private final WriteRelationshipService writeRelationshipService;
    private final UserSpecificConfigurationParser configurationParser;

    public PushbackPathFindingWriteProcedureFacade(
        PathFindingComputeBusinessFacade businessFacade,
        RequestScopedDependencies requestScopedDependencies,
        WriteContext writeContext, Log log,
        WriteRelationshipService writeRelationshipService,
        UserSpecificConfigurationParser configurationParser
    ) {
        this.businessFacade = businessFacade;
        this.requestScopedDependencies = requestScopedDependencies;
        this.writeContext = writeContext;
        this.log = log;
        this.writeRelationshipService = writeRelationshipService;
        this.configurationParser = configurationParser;
    }

    public Stream<BellmanFordWriteResult> bellmanFord(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsBellmanFordWriteConfig::of
        );

        var writeStep = new BellmanFordWriteStep(
            writeRelationshipService,
            config.writeRelationshipType(),
            config.writeNegativeCycles(),
            config.writeNodeIds(),
            config.writeCosts(),
            config::resolveResultStore,
            config.jobId()
        );

        return businessFacade.bellmanFord(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new BellmanFordWriteResultTransformerBuilder(writeStep, config)
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

        var writeStep = new ShortestPathWriteStep(
            writeRelationshipService,
            config.writeRelationshipType(),
            config.writeNodeIds(),
            config.writeCosts(),
            config::resolveResultStore,
            config.jobId()
        );


        return businessFacade.deltaStepping(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new ShortestPathWriteResultTransformerBuilder(writeStep, config.jobId(), config.toMap())
        ).join();
    }

    public Stream<KSpanningTreeWriteResult> kSpanningTree(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            KSpanningTreeWriteConfig::of
        );

        var writeStep = new KSpanningTreeWriteStep(
            config.writeProperty(),
            writeContext,
            config::resolveResultStore,
            config.jobId(),
            config.writeConcurrency(),
            log,
            requestScopedDependencies.taskRegistryFactory(),
            requestScopedDependencies.terminationFlag()
        );

        return businessFacade.kSpanningTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toKSpanningTreeParameters(),
            config.jobId(),
            config.logProgress(),
            new KSpanningTreeWriteResultTransformerBuilder(writeStep, config)
        ).join();
    }


    public Stream<PrizeCollectingSteinerTreeWriteResult> pcst(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            PCSTWriteConfig::of
        );
        var writeStep = new PrizeCollectingSteinerTreeWriteStep(
            writeRelationshipService,
            config.writeRelationshipType(),
            config.writeProperty(),
            config::resolveResultStore,
            config.jobId()
        );

        return businessFacade.pcst(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new PCSTWriteResultTransformerBuilder(writeStep, config)
        ).join();
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathAStar(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            ShortestPathAStarWriteConfig::of
        );

        var writeStep = new ShortestPathWriteStep(
            writeRelationshipService,
            config.writeRelationshipType(),
            config.writeNodeIds(),
            config.writeCosts(),
            config::resolveResultStore,
            config.jobId()
        );

        return businessFacade.singlePairShortestPathAStar(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new ShortestPathWriteResultTransformerBuilder(writeStep, config.jobId(), config.toMap())
        ).join();
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathDijkstra(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            ShortestPathDijkstraWriteConfig::of
        );

        var writeStep = new ShortestPathWriteStep(
            writeRelationshipService,
            config.writeRelationshipType(),
            config.writeNodeIds(),
            config.writeCosts(),
            config::resolveResultStore,
            config.jobId()
        );

        return businessFacade.singlePairShortestPathDijkstra(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new ShortestPathWriteResultTransformerBuilder(writeStep, config.jobId(), config.toMap())
        ).join();
    }

    public Stream<StandardWriteRelationshipsResult> singlePairShortestPathYens(
        String graphName,
        Map<String, Object> configuration
    ) {

        var config = configurationParser.parseConfiguration(
            configuration,
            ShortestPathYensWriteConfig::of
        );

        var writeStep = new ShortestPathWriteStep(
            writeRelationshipService,
            config.writeRelationshipType(),
            config.writeNodeIds(),
            config.writeCosts(),
            config::resolveResultStore,
            config.jobId()
        );

        return businessFacade.singlePairShortestPathYens(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new ShortestPathWriteResultTransformerBuilder(writeStep, config.jobId(), config.toMap())
        ).join();
    }

    public Stream<StandardWriteRelationshipsResult> singleSourceShortestPathDijkstra(
        String graphName,
        Map<String, Object> configuration
    ) {

        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsDijkstraWriteConfig::of
        );

        var writeStep = new ShortestPathWriteStep(
            writeRelationshipService,
            config.writeRelationshipType(),
            config.writeNodeIds(),
            config.writeCosts(),
            config::resolveResultStore,
            config.jobId()
        );

        return businessFacade.singleSourceShortestPathDijkstra(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toSingleSourceParameters(),
            config.jobId(),
            config.logProgress(),
            new ShortestPathWriteResultTransformerBuilder(writeStep, config.jobId(), config.toMap())
        ).join();
    }

    public Stream<SpanningTreeWriteResult> spanningTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SpanningTreeWriteConfig::of
        );

        var writeStep = new SpanningTreeWriteStep(
            writeRelationshipService,
            config.writeRelationshipType(),
            config.writeProperty(),
            config::resolveResultStore,
            config.jobId()
        );

        return businessFacade.spanningTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new SpanningTreeWriteResultTransformerBuilder(writeStep, config)
        ).join();
    }

    public Stream<SteinerWriteResult> steinerTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SteinerTreeWriteConfig::of
        );

        var writeStep = new SteinerTreeWriteStep(
            writeRelationshipService,
            config.sourceNode(),
            config.writeRelationshipType(),
            config.writeProperty(),
            config::resolveResultStore,
            config.jobId()
        );

        return businessFacade.steinerTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new SteinerTreeWriteResultTransformerBuilder(writeStep, config)
        ).join();

    }

}
