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
package org.neo4j.gds.procedures.algorithms.pathfinding.mutate;

import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.machinery.MutateNodePropertyService;
import org.neo4j.gds.applications.algorithms.machinery.MutateRelationshipService;
import org.neo4j.gds.maxflow.MaxFlowMutateConfig;
import org.neo4j.gds.mcmf.MCMFMutateConfig;
import org.neo4j.gds.pathfinding.MCMFMutateStep;
import org.neo4j.gds.pathfinding.PathFindingComputeBusinessFacade;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarMutateConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordMutateConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraMutateConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraMutateConfig;
import org.neo4j.gds.paths.traverse.BfsMutateConfig;
import org.neo4j.gds.paths.traverse.DfsMutateConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensMutateConfig;
import org.neo4j.gds.pcst.PCSTMutateConfig;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.BellmanFordMutateResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.MCMFMutateResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.MaxFlowMutateResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingMutateResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.PrizeCollectingSteinerTreeMutateResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.RandomWalkMutateResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeMutateResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.SteinerMutateResult;
import org.neo4j.gds.spanningtree.SpanningTreeMutateConfig;
import org.neo4j.gds.steiner.SteinerTreeMutateConfig;
import org.neo4j.gds.traversal.RandomWalkMutateConfig;

import java.util.Map;
import java.util.stream.Stream;

public final class PushbackPathFindingMutateProcedureFacade {

    private final PathFindingComputeBusinessFacade businessFacade;

    private final UserSpecificConfigurationParser configurationParser;

    private final MutateRelationshipService mutateRelationshipService;
    private final MutateNodePropertyService mutateNodePropertyService;


    public PushbackPathFindingMutateProcedureFacade(
        PathFindingComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        MutateRelationshipService mutateRelationshipService,
        MutateNodePropertyService mutateNodePropertyService
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.mutateRelationshipService = mutateRelationshipService;
        this.mutateNodePropertyService = mutateNodePropertyService;
    }

    public Stream<BellmanFordMutateResult> bellmanFord(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsBellmanFordMutateConfig::of
        );

        return businessFacade.bellmanFord(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new BellmanFordMutateResultTransformerBuilder(
                mutateRelationshipService,
                config
            )
        ).join();
    }

    public Stream<PathFindingMutateResult> breadthFirstSearch(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            BfsMutateConfig::of
        );

        return businessFacade.breadthFirstSearch(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new TraverseMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }

    public Stream<PathFindingMutateResult> deltaStepping(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsDeltaMutateConfig::of
        );

        return businessFacade.deltaStepping(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new PathFindingMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }

    public Stream<PathFindingMutateResult> depthFirstSearch(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            DfsMutateConfig::of
        );

        return businessFacade.depthFirstSearch(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new TraverseMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }

    public Stream<MaxFlowMutateResult> maxFlow(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            MaxFlowMutateConfig::of
        );

        return businessFacade.maxFlow(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new MaxFlowMutateResultTransformerBuilder(mutateRelationshipService, config)
        ).join();
    }

    public Stream<MCMFMutateResult> mcmf(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            MCMFMutateConfig::of
        );

        var mutateStep =new MCMFMutateStep(
            config.mutateRelationshipType(),
            config.mutateProperty(),
            mutateRelationshipService
        );

        return businessFacade.mcmf(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.costProperty(),
            config.toMCMFParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new MCMFMutateResultTransformer(
                mutateStep,
                graphResources.graph(),
                graphResources.graphStore(),
                config.toMap()
            )
        ).join();
    }

    public Stream<PrizeCollectingSteinerTreeMutateResult> prizeCollectingSteinerTree(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            PCSTMutateConfig::of
        );

        return businessFacade.pcst(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new PrizeSteinerMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }

    public Stream<RandomWalkMutateResult> randomWalk(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            RandomWalkMutateConfig::of
        );

        return businessFacade.randomWalkCountingNodeVisits(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new RandomWalkMutateResultTransformerBuilder(mutateNodePropertyService,config)
        ).join();
    }

    public Stream<PathFindingMutateResult> singlePairShortestPathAStar(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            ShortestPathAStarMutateConfig::of
        );

        return businessFacade.singlePairShortestPathAStar(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new PathFindingMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }

    public Stream<PathFindingMutateResult> singlePairShortestPathDijkstra(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            ShortestPathDijkstraMutateConfig::of
        );

        return businessFacade.singlePairShortestPathDijkstra(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new PathFindingMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }

    public Stream<PathFindingMutateResult> singlePairShortestPathYens(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            ShortestPathYensMutateConfig::of
        );

        return businessFacade.singlePairShortestPathYens(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new PathFindingMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }

    public Stream<PathFindingMutateResult> singleSourceShortestPathDijkstra(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsDijkstraMutateConfig::of
        );

        return businessFacade.singleSourceShortestPathDijkstra(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toSingleSourceParameters(),
            config.jobId(),
            config.logProgress(),
            new PathFindingMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }

    public Stream<SpanningTreeMutateResult> spanningTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SpanningTreeMutateConfig::of
        );

        return businessFacade.spanningTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new SpanningTreeMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }

    public Stream<SteinerMutateResult> steinerTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SteinerTreeMutateConfig::of
        );

        return businessFacade.steinerTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new SteinerTreeMutateResultTransformerBuilder(mutateRelationshipService,config)
        ).join();
    }
}
