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
package org.neo4j.gds.procedures.algorithms.pathfinding.stream;

import org.neo4j.gds.allshortestpaths.AllShortestPathsConfig;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.dag.longestPath.DagLongestPathStreamConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortStreamConfig;
import org.neo4j.gds.maxflow.MaxFlowStreamConfig;
import org.neo4j.gds.mcmf.MCMFStreamConfig;
import org.neo4j.gds.pathfinding.PathFindingComputeBusinessFacade;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarStreamConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordStreamConfig;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.AllShortestPathsDijkstraStreamConfig;
import org.neo4j.gds.paths.dijkstra.config.ShortestPathDijkstraStreamConfig;
import org.neo4j.gds.paths.traverse.BfsStreamConfig;
import org.neo4j.gds.paths.traverse.DfsStreamConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensStreamConfig;
import org.neo4j.gds.pcst.PCSTStreamConfig;
import org.neo4j.gds.procedures.algorithms.configuration.UserSpecificConfigurationParser;
import org.neo4j.gds.procedures.algorithms.pathfinding.BellmanFordStreamResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.MaxFlowStreamResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.PathFindingStreamResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.RandomWalkStreamResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.SpanningTreeStreamResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.TopologicalSortStreamResult;
import org.neo4j.gds.procedures.algorithms.pathfinding.TraversalStreamResult;
import org.neo4j.gds.result.TimedAlgorithmResult;
import org.neo4j.gds.spanningtree.SpanningTreeStreamConfig;
import org.neo4j.gds.steiner.SteinerTreeStreamConfig;
import org.neo4j.gds.traversal.RandomWalkStreamConfig;

import java.util.Map;
import java.util.stream.Stream;

public final class PushbackPathFindingStreamProcedureFacade {
    private final PathFindingComputeBusinessFacade businessFacade;

    private final UserSpecificConfigurationParser configurationParser;

    private final CloseableResourceRegistry closeableResourceRegistry;
    private final NodeLookup nodeLookup;
    private final ProcedureReturnColumns procedureReturnColumns;

    public PushbackPathFindingStreamProcedureFacade(
        PathFindingComputeBusinessFacade businessFacade,
        UserSpecificConfigurationParser configurationParser,
        CloseableResourceRegistry closeableResourceRegistry,
        NodeLookup nodeLookup,
        ProcedureReturnColumns procedureReturnColumns
    ) {
        this.businessFacade = businessFacade;
        this.configurationParser = configurationParser;
        this.closeableResourceRegistry = closeableResourceRegistry;
        this.nodeLookup = nodeLookup;
        this.procedureReturnColumns = procedureReturnColumns;
    }

    public Stream<AllShortestPathsStreamResult> allShortestPaths(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(configuration, AllShortestPathsConfig::of);

        return businessFacade.allShortestPaths(
                GraphName.parse(graphName),
                config.toGraphParameters(),
                config.relationshipWeightProperty(),
                config.toParameters(),
                config.jobId(),
                // `MSBFSASPAlgorithm` implementations already maps the ids to the original ones => no need for transformation
                (graphResources) -> TimedAlgorithmResult::result
            )
            .join();
    }

    public Stream<BellmanFordStreamResult> bellmanFord(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsBellmanFordStreamConfig::of
        );

        var routeRequested = procedureReturnColumns.contains("route");
        var resultTransformerBuilder = new BellmanFordStreamResultTransformerBuilder(
            closeableResourceRegistry,
            nodeLookup,
            routeRequested
        );

        return businessFacade.bellmanFord(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            resultTransformerBuilder
        ).join();
    }

    public Stream<TraversalStreamResult> breadthFirstSearch(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            BfsStreamConfig::of
        );
        var routeRequested = procedureReturnColumns.contains("path");

        var traversalResultTransformerBuilder = new TraversalStreamResultTransformerBuilder(
            nodeLookup,
            routeRequested,
            config.sourceNode()
        );

        return businessFacade.breadthFirstSearch(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            traversalResultTransformerBuilder
        ).join();
    }

    public Stream<PathFindingStreamResult> deltaStepping(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsDeltaStreamConfig::of
        );
        var routeRequested = procedureReturnColumns.contains("path");

        var pathFindingResultTransformerBuilder = new PathFindingStreamResultTransformerBuilder(
            closeableResourceRegistry,
            nodeLookup,
            routeRequested
        );

        return businessFacade.deltaStepping(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            pathFindingResultTransformerBuilder
        ).join();
    }

    public Stream<TraversalStreamResult> depthFirstSearch(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            DfsStreamConfig::of
        );
        var routeRequested = procedureReturnColumns.contains("path");

        var traversalResultTransformerBuilder = new TraversalStreamResultTransformerBuilder(
            nodeLookup,
            routeRequested,
            config.sourceNode()
        );

        return businessFacade.depthFirstSearch(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            traversalResultTransformerBuilder
        ).join();
    }

    public Stream<PathFindingStreamResult> longestPath(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            DagLongestPathStreamConfig::of
        );

        var pathFindingResultTransformerBuilder = new PathFindingStreamResultTransformerBuilder(
            closeableResourceRegistry,
            nodeLookup,
            procedureReturnColumns.contains("path")
        );

        return businessFacade.longestPath(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            pathFindingResultTransformerBuilder
        ).join();
    }

    public Stream<MaxFlowStreamResult> maxFlow(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            MaxFlowStreamConfig::of
        );

        var maxFlowResultTransformerBuilder = new MaxFlowStreamResultTransformerBuilder();
        return businessFacade.maxFlow(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            maxFlowResultTransformerBuilder
        ).join();
    }

    public Stream<MaxFlowStreamResult> mcmf(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            MCMFStreamConfig::of
        );

        return businessFacade.mcmf(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.costProperty(),
            config.toMCMFParameters(),
            config.jobId(),
            config.logProgress(),
            graphResources -> new MCMFStreamResultTransformer(graphResources.graphStore().nodes())
        ).join();
    }

    public Stream<SpanningTreeStreamResult> prizeCollectingSteinerTree(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            PCSTStreamConfig::of
        );

        return businessFacade.pcst(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new PrizeCollectingSteinerTreeStreamResultTransformerBuilder()
        ).join();
    }

    public Stream<RandomWalkStreamResult> randomWalk(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            RandomWalkStreamConfig::of
        );
        var routeRequested = procedureReturnColumns.contains("path");

        var randomWalkStreamResultTransformerBuilder = new RandomWalkStreamResultTransformerBuilder(
            closeableResourceRegistry,
            nodeLookup,
            routeRequested
        );

        return businessFacade.randomWalk(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            randomWalkStreamResultTransformerBuilder
        ).join();
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathAStar(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            ShortestPathAStarStreamConfig::of
        );
        var routeRequested = procedureReturnColumns.contains("path");

        var pathFindingResultTransformerBuilder = new PathFindingStreamResultTransformerBuilder(
            closeableResourceRegistry,
            nodeLookup,
            routeRequested
        );

        return businessFacade.singlePairShortestPathAStar(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            pathFindingResultTransformerBuilder
        ).join();
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathDijkstra(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            ShortestPathDijkstraStreamConfig::of
        );
        var routeRequested = procedureReturnColumns.contains("path");


        var pathFindingResultTransformerBuilder = new PathFindingStreamResultTransformerBuilder(
            closeableResourceRegistry,
            nodeLookup,
            routeRequested
        );

        return businessFacade.singlePairShortestPathDijkstra(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            pathFindingResultTransformerBuilder
        ).join();
    }

    public Stream<PathFindingStreamResult> singlePairShortestPathYens(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            ShortestPathYensStreamConfig::of
        );
        var routeRequested = procedureReturnColumns.contains("path");

        var pathFindingResultTransformerBuilder = new PathFindingStreamResultTransformerBuilder(
            closeableResourceRegistry,
            nodeLookup,
            routeRequested
        );

        return businessFacade.singlePairShortestPathYens(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            pathFindingResultTransformerBuilder
        ).join();
    }

    public Stream<PathFindingStreamResult> singleSourceShortestPathDijkstra(
        String graphName,
        Map<String, Object> configuration
    ) {
        var config = configurationParser.parseConfiguration(
            configuration,
            AllShortestPathsDijkstraStreamConfig::of
        );
        var routeRequested = procedureReturnColumns.contains("path");


        var pathFindingResultTransformerBuilder = new PathFindingStreamResultTransformerBuilder(
            closeableResourceRegistry,
            nodeLookup,
            routeRequested
        );

        return businessFacade.singleSourceShortestPathDijkstra(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toSingleSourceParameters(),
            config.jobId(),
            config.logProgress(),
            pathFindingResultTransformerBuilder
        ).join();
    }

    public Stream<SpanningTreeStreamResult> spanningTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SpanningTreeStreamConfig::of
        );
        var resultTransformerBuilder  = new SpanningTreeStreamResultTransformerBuilder(config.sourceNode());
        return businessFacade.spanningTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            resultTransformerBuilder
        ).join();
    }

    public Stream<SpanningTreeStreamResult> steinerTree(String graphName, Map<String, Object> configuration) {
        var config = configurationParser.parseConfiguration(
            configuration,
            SteinerTreeStreamConfig::of
        );
        var resultTransformerBuilder  = new SteinerTreeStreamResultTransformerBuilder(config.sourceNode());
        return businessFacade.steinerTree(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.relationshipWeightProperty(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            resultTransformerBuilder
        ).join();
    }

    public Stream<TopologicalSortStreamResult> topologicalSort(
        String graphName,
        Map<String, Object> configuration
    ) {

        var config = configurationParser.parseConfiguration(
            configuration,
            TopologicalSortStreamConfig::of
        );

        return businessFacade.topologicalSort(
            GraphName.parse(graphName),
            config.toGraphParameters(),
            config.toParameters(),
            config.jobId(),
            config.logProgress(),
            new TopologicalSortStreamResultTransformerBuilder()
        ).join();
    }

}
