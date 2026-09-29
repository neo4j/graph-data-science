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

import org.neo4j.gds.allshortestpaths.AllShortestPathsConfig;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.core.loading.validation.GraphLoadedETL;
import org.neo4j.gds.core.loading.validation.RelationshipPropertyGraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodeTargetNodesGraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.dag.longestPath.DagLongestPathBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowBaseConfig;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.mcmf.MCMFBaseConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public class InstrumentedPathFindingAlgorithms {
    private final TrackedPathFindingAlgorithms algorithms;
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;

    public InstrumentedPathFindingAlgorithms(
        TrackedPathFindingAlgorithms algorithms,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        this.algorithms = algorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> allShortestPaths(
        GraphName graphName,
        AllShortestPathsConfig configuration,
        Optional<SideEffect<Stream<AllShortestPathsStreamResult>, METADATA>> sideEffect,
        ResultRenderer<Stream<AllShortestPathsStreamResult>, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.allShortestPaths(graph, configuration),
            () -> estimationFacade.allShortestPaths(configuration),
            AlgorithmLabel.AllShortestPaths,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> bfs(
        GraphName graphName,
        BfsBaseConfig configuration,
        Optional<SideEffect<HugeLongArray, METADATA>> sideEffect,
        ResultRenderer<HugeLongArray, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(
                new SourceNodeTargetNodesGraphStoreValidation(configuration.sourceNode(), configuration.targetNodes())
            );

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.bfs(graph, configuration),
            estimationFacade::breadthFirstSearch,
            AlgorithmLabel.BFS,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> dfs(
        GraphName graphName,
        DfsBaseConfig configuration,
        Optional<SideEffect<HugeLongArray, METADATA>> sideEffect,
        ResultRenderer<HugeLongArray, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(
                new SourceNodeTargetNodesGraphStoreValidation(configuration.sourceNode(), configuration.targetNodes())
            );

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.dfs(graph, configuration),
            estimationFacade::depthFirstSearch,
            AlgorithmLabel.DFS,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> longestPath(
        GraphName graphName,
        DagLongestPathBaseConfig configuration,
        Optional<SideEffect<PathFindingResult, METADATA>> sideEffect,
        ResultRenderer<PathFindingResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.longestPath(graph, configuration),
            estimationFacade::longestPath,
            AlgorithmLabel.LongestPath,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> maxFlow(
        GraphName graphName,
        MaxFlowBaseConfig configuration,
        Optional<SideEffect<FlowResult, METADATA>> sideEffect,
        ResultRenderer<FlowResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(FlowAlgorithmRequirements.create(configuration.toMaxFlowParameters()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.maxFlow(graph, configuration),
            () -> estimationFacade.maxFlow(configuration),
            AlgorithmLabel.MaxFlow,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> minCostMaxFlow(
        GraphName graphName,
        MCMFBaseConfig configuration,
        Optional<SideEffect<CostFlowResult, METADATA>> sideEffect,
        ResultRenderer<CostFlowResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(FlowAlgorithmRequirements.create(configuration.toMaxFlowParameters()));

        var secondGraphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new RelationshipPropertyGraphStoreValidation(configuration.costProperty(), "costProperty"));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            false,
            graphLoadETLBuilder,
            configuration.costProperty(),
            Optional.of(secondGraphLoadETLBuilder),
            (graph, secondGraph, __) -> algorithms.minCostMaxFlow(graph, secondGraph.orElseThrow(), configuration),
            () -> estimationFacade.mcmf(configuration),
            AlgorithmLabel.MinCostMaxFlow,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> randomWalk(
        GraphName graphName,
        RandomWalkBaseConfig configuration,
        Optional<SideEffect<Stream<long[]>, METADATA>> sideEffect,
        ResultRenderer<Stream<long[]>, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with((graphStore, selectedLabels, selectedRelationshipTypes, relationshipProperty) -> new SourceNodesRequirement(configuration.sourceNodes()).validate(graphStore, selectedLabels, selectedRelationshipTypes))
            .with((GraphLoadedETL) graph -> new RandomWalkGraphValidation(configuration.concurrency(), DefaultPool.INSTANCE).validate(graph));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.randomWalk(graph, configuration),
            () -> estimationFacade.randomWalk(configuration),
            AlgorithmLabel.RandomWalk,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> singlePairShortestPathDijkstra(
        GraphName graphName,
        DijkstraSourceTargetsBaseConfig configuration,
        Optional<SideEffect<PathFindingResult, METADATA>> sideEffect,
        ResultRenderer<PathFindingResult, RESULT, METADATA> resultRenderer,
        boolean skipRelationshipProperty
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(
                new SourceNodeTargetNodesGraphStoreValidation(configuration.sourceNode(), configuration.targetsList())
            )
            .with(new GraphLoadedETL() {
                @Override
                public void onGraphLoaded(Graph graph) {
                    // rename this, it is clearly generic
                    new RandomWalkGraphValidation(configuration.concurrency(), DefaultPool.INSTANCE).validate(graph);
                }
            });

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            skipRelationshipProperty,
            graphLoadETLBuilder,
            Optional.empty(),
            Optional.empty(),
            (graph, __, ___) -> algorithms.singlePairShortestPathDijkstra(graph, configuration),
            () -> estimationFacade.singlePairShortestPathDijkstra(configuration),
            AlgorithmLabel.Dijkstra,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> steinerTree(
        GraphName graphName,
        SteinerTreeBaseConfig configuration,
        Optional<SideEffect<SteinerTreeResult, METADATA>> sideEffect,
        ResultRenderer<SteinerTreeResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(
                new SourceNodeTargetNodesGraphStoreValidation(configuration.sourceNode(), configuration.targetNodes())
            );

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.steinerTree(graph, configuration),
            () -> estimationFacade.steinerTree(configuration),
            AlgorithmLabel.SteinerTree,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> topologicalSort(
        GraphName graphName,
        TopologicalSortBaseConfig configuration,
        Optional<SideEffect<TopologicalSortResult, METADATA>> sideEffect,
        ResultRenderer<TopologicalSortResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.topologicalSort(graph, configuration),
            estimationFacade::topologicalSort,
            AlgorithmLabel.TopologicalSort,
            sideEffect,
            resultRenderer
        );
    }
}
