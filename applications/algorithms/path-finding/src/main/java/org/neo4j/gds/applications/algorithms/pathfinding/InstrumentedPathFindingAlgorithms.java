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

import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.allshortestpaths.AllShortestPathsConfig;
import org.neo4j.gds.allshortestpaths.AllShortestPathsStreamResult;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.applications.algorithms.execution.LaunchConvenience;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.applications.algorithms.machinery.ProgressTrackerCreator;
import org.neo4j.gds.applications.algorithms.machinery.RequestScopedDependencies;
import org.neo4j.gds.applications.algorithms.machinery.ResultRenderer;
import org.neo4j.gds.applications.algorithms.machinery.SideEffect;
import org.neo4j.gds.collections.ha.HugeLongArray;
import org.neo4j.gds.collections.haa.HugeAtomicLongArray;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.validation.GraphLoadETLBuilder;
import org.neo4j.gds.core.loading.validation.GraphStoreFilteredETL;
import org.neo4j.gds.core.loading.validation.NodePropertyMustExistOnAllLabels;
import org.neo4j.gds.core.loading.validation.NodePropertyTypeRequirement;
import org.neo4j.gds.core.loading.validation.NonNegativeWeightsOnlyETL;
import org.neo4j.gds.core.loading.validation.RelationshipPropertyGraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodeRequirement;
import org.neo4j.gds.core.loading.validation.SourceNodeTargetNodeGraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodeTargetNodesGraphStoreValidation;
import org.neo4j.gds.core.loading.validation.SourceNodesRequirement;
import org.neo4j.gds.core.loading.validation.UndirectedOnlyRequirement;
import org.neo4j.gds.core.utils.logging.GdsLoggers;
import org.neo4j.gds.dag.longestPath.DagLongestPathBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortBaseConfig;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.kspanningtree.KSpanningTreeBaseConfig;
import org.neo4j.gds.maxflow.FlowResult;
import org.neo4j.gds.maxflow.MaxFlowBaseConfig;
import org.neo4j.gds.mcmf.CostFlowResult;
import org.neo4j.gds.mcmf.MCMFBaseConfig;
import org.neo4j.gds.paths.astar.config.ShortestPathAStarBaseConfig;
import org.neo4j.gds.paths.bellmanford.AllShortestPathsBellmanFordBaseConfig;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.config.AllShortestPathsDeltaBaseConfig;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.dijkstra.config.DijkstraBaseConfig;
import org.neo4j.gds.paths.dijkstra.config.DijkstraSourceTargetsBaseConfig;
import org.neo4j.gds.paths.traverse.BfsBaseConfig;
import org.neo4j.gds.paths.traverse.DfsBaseConfig;
import org.neo4j.gds.paths.yens.config.ShortestPathYensBaseConfig;
import org.neo4j.gds.pcst.PCSTBaseConfig;
import org.neo4j.gds.pricesteiner.PrizeCollectingSteinerTreeResult;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeBaseConfig;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.traversal.RandomWalkBaseConfig;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

public final class InstrumentedPathFindingAlgorithms {
    private final TrackedPathFindingAlgorithms algorithms;
    private final PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade;
    private final LaunchConvenience launchConvenience;

    private InstrumentedPathFindingAlgorithms(
        TrackedPathFindingAlgorithms algorithms,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        this.algorithms = algorithms;
        this.estimationFacade = estimationFacade;
        this.launchConvenience = launchConvenience;
    }

    public static InstrumentedPathFindingAlgorithms create(
        GdsLoggers loggers,
        RequestScopedDependencies requestScopedDependencies,
        ProgressTrackerCreator progressTrackerCreator,
        PathFindingAlgorithmsEstimationModeBusinessFacade estimationFacade,
        LaunchConvenience launchConvenience
    ) {
        var algorithms = new PathFindingAlgorithms(loggers.log());

        var progressTrackerFactory = new ProgressTrackerFactory(
            loggers.log(),
            loggers.loggerForProgressTracking(),
            requestScopedDependencies.correlationId(),
            requestScopedDependencies.taskRegistryFactory()
        );

        var trackedPathFindingAlgorithms = new TrackedPathFindingAlgorithms(
            algorithms,
            requestScopedDependencies,
            progressTrackerCreator,
            progressTrackerFactory
        );

        return new InstrumentedPathFindingAlgorithms(
            trackedPathFindingAlgorithms,
            estimationFacade,
            launchConvenience
        );
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

    public <RESULT, METADATA> CompletableFuture<RESULT> bellmanFord(
        GraphName graphName,
        AllShortestPathsBellmanFordBaseConfig configuration,
        Optional<SideEffect<BellmanFordResult, METADATA>> sideEffect,
        ResultRenderer<BellmanFordResult, RESULT, METADATA> resultRenderer
    ) {
        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            GraphLoadETLBuilder.empty(),
            (graph, __, ___) -> algorithms.bellmanFord(graph, configuration),
            () -> estimationFacade.bellmanFord(configuration),
            AlgorithmLabel.BellmanFord,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> breadthFirstSearch(
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
            (graph, __, ___) -> algorithms.breadthFirstSearch(graph, configuration),
            estimationFacade::breadthFirstSearch,
            AlgorithmLabel.BFS,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> deltaStepping(
        GraphName graphName,
        AllShortestPathsDeltaBaseConfig configuration,
        Optional<SideEffect<PathFindingResult, METADATA>> sideEffect,
        ResultRenderer<PathFindingResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new SourceNodeRequirement(configuration.sourceNode())
                        .validate(graphStore, selectedLabels, selectedRelationshipTypes);
                }
            });

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.deltaStepping(graph, configuration),
            estimationFacade::deltaStepping,
            AlgorithmLabel.DeltaStepping,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> depthFirstSearch(
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
            (graph, __, ___) -> algorithms.depthFirstSearch(graph, configuration),
            estimationFacade::depthFirstSearch,
            AlgorithmLabel.DFS,
            sideEffect,
            resultRenderer
        );
    }

    public <RESULT, METADATA> CompletableFuture<RESULT> kSpanningTree(
        GraphName graphName,
        KSpanningTreeBaseConfig configuration,
        Optional<SideEffect<SpanningTree, METADATA>> sideEffect,
        ResultRenderer<SpanningTree, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new SourceNodeRequirement(configuration.sourceNode())
                        .validate(graphStore, selectedLabels, selectedRelationshipTypes);
                }
            })
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new UndirectedOnlyRequirement("K-Spanning Tree")
                        .validate(graphStore, selectedLabels, selectedRelationshipTypes);
                }
            });

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.kSpanningTree(graph, configuration),
            estimationFacade::kSpanningTree,
            AlgorithmLabel.KSpanningTree,
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

    public <RESULT, METADATA> CompletableFuture<RESULT> prizeCollectingSteinerTree(
        GraphName graphName,
        PCSTBaseConfig configuration,
        Optional<SideEffect<PrizeCollectingSteinerTreeResult, METADATA>> sideEffect,
        ResultRenderer<PrizeCollectingSteinerTreeResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new UndirectedOnlyRequirement("Prize-collecting Steiner Tree")
                        .validate(graphStore, selectedLabels, selectedRelationshipTypes);
                }
            })
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new NodePropertyMustExistOnAllLabels(configuration.prizeProperty())
                        .validate(graphStore, selectedLabels, selectedRelationshipTypes);
                }
            })
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new NodePropertyTypeRequirement(
                        configuration.prizeProperty(),
                        List.of(ValueType.DOUBLE)
                    ).validate(graphStore, selectedLabels, selectedRelationshipTypes);
                }
            });

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.prizeCollectingSteinerTree(graph, configuration),
            estimationFacade::prizeCollectingSteinerTree,
            AlgorithmLabel.PrizeCollectingSteinerTree,
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
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new SourceNodesRequirement(configuration.sourceNodes()).validate(
                        graphStore,
                        selectedLabels,
                        selectedRelationshipTypes
                    );
                }
            })
            .with(new NonNegativeWeightsOnlyETL(configuration.concurrency(), DefaultPool.INSTANCE));

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

    /**
     * Here we have a variant of RandomWalk where, for unknown reasons, we did not reuse the original RandomWalk code.
     * We might have copied it and made a few changes to satisfy the "CountingNodeVisits" bit. But looking at it today,
     * it is not obvious to me how to de-duplicate this. So I am leaving it as an exercise for the next person.
     * Also, it was never used so perhaps better to scrap it?
     * Anyway, it might have been built for a reason, so here it is.
     */
    public <RESULT, METADATA> CompletableFuture<RESULT> randomWalkCountingNodeVisits(
        GraphName graphName,
        RandomWalkBaseConfig configuration,
        Optional<SideEffect<HugeAtomicLongArray, METADATA>> sideEffect,
        ResultRenderer<HugeAtomicLongArray, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new SourceNodesRequirement(configuration.sourceNodes()).validate(
                        graphStore,
                        selectedLabels,
                        selectedRelationshipTypes
                    );
                }
            })
            .with(new RandomWalkGraphValidation(configuration.concurrency(), DefaultPool.INSTANCE));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.randomWalkCountingNodeVisits(graph, configuration),
            () -> estimationFacade.randomWalkCountingVisits(configuration),
            AlgorithmLabel.RandomWalk, // nope, they didn't create a new label
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> singlePairShortestPathAStar(
        GraphName graphName,
        ShortestPathAStarBaseConfig configuration,
        Optional<SideEffect<PathFindingResult, METADATA>> sideEffect,
        ResultRenderer<PathFindingResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new SourceNodeTargetNodeGraphStoreValidation(configuration.sourceNode(), configuration.targetNode()))
            .with(new NonNegativeWeightsOnlyETL(configuration.concurrency(), DefaultPool.INSTANCE));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.singlePairShortestPathAStar(graph, configuration),
            estimationFacade::singlePairShortestPathAStar,
            AlgorithmLabel.AStar,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> singlePairShortestPathDijkstra(
        GraphName graphName,
        DijkstraSourceTargetsBaseConfig configuration,
        Optional<SideEffect<PathFindingResult, METADATA>> sideEffect,
        ResultRenderer<PathFindingResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new SourceNodeTargetNodesGraphStoreValidation(configuration.sourceNode(), configuration.targetsList()))
            .with(new NonNegativeWeightsOnlyETL(configuration.concurrency(), DefaultPool.INSTANCE));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
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

    <RESULT, METADATA> CompletableFuture<RESULT> singlePairShortestPathYens(
        GraphName graphName,
        ShortestPathYensBaseConfig configuration,
        Optional<SideEffect<PathFindingResult, METADATA>> sideEffect,
        ResultRenderer<PathFindingResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new SourceNodeTargetNodeGraphStoreValidation(configuration.sourceNode(), configuration.targetNode()));

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.singlePairShortestPathYens(graph, configuration),
            () -> estimationFacade.singlePairShortestPathYens(configuration),
            AlgorithmLabel.Yens,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> singleSourceShortestPathDijkstra(
        GraphName graphName,
        DijkstraBaseConfig configuration,
        Optional<SideEffect<PathFindingResult, METADATA>> sideEffect,
        ResultRenderer<PathFindingResult, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new SourceNodeRequirement(configuration.sourceNode())
                        .validate(graphStore, selectedLabels, selectedRelationshipTypes);
                }
            });

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.singleSourceShortestPathDijkstra(graph, configuration),
            () -> estimationFacade.singleSourceShortestPathDijkstra(configuration),
            AlgorithmLabel.SingleSourceDijkstra,
            sideEffect,
            resultRenderer
        );
    }

    <RESULT, METADATA> CompletableFuture<RESULT> spanningTree(
        GraphName graphName,
        SpanningTreeBaseConfig configuration,
        Optional<SideEffect<SpanningTree, METADATA>> sideEffect,
        ResultRenderer<SpanningTree, RESULT, METADATA> resultRenderer
    ) {
        var graphLoadETLBuilder = new GraphLoadETLBuilder()
            .with(
                new GraphStoreFilteredETL() {
                    @Override
                    public void onGraphStoreFiltered(
                        GraphStore graphStore,
                        Collection<NodeLabel> selectedLabels,
                        Collection<RelationshipType> selectedRelationshipTypes,
                        Optional<String> relationshipProperty
                    ) {
                        new SourceNodeRequirement(configuration.sourceNode()).validate(graphStore, selectedLabels, selectedRelationshipTypes);
                    }
                }
            )
            .with(new GraphStoreFilteredETL() {
                @Override
                public void onGraphStoreFiltered(
                    GraphStore graphStore,
                    Collection<NodeLabel> selectedLabels,
                    Collection<RelationshipType> selectedRelationshipTypes,
                    Optional<String> relationshipProperty
                ) {
                    new UndirectedOnlyRequirement("Spanning Tree").validate(graphStore, selectedLabels, selectedRelationshipTypes);
                }
            });

        return launchConvenience.launchAlgorithm(
            graphName,
            configuration,
            graphLoadETLBuilder,
            (graph, __, ___) -> algorithms.spanningTree(graph, configuration),
            estimationFacade::spanningTree,
            AlgorithmLabel.SpanningTree,
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
