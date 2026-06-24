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
package org.neo4j.gds;

import org.neo4j.gds.api.Graph;
import org.neo4j.gds.applications.algorithms.machinery.AlgorithmLabel;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.progress.tasks.Task;
import org.neo4j.gds.dag.longestPath.LongestPathTask;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortTask;
import org.neo4j.gds.kspanningtree.KSpanningTreeTask;
import org.neo4j.gds.maxflow.MaxFlowTask;
import org.neo4j.gds.mcmf.MinCostMaxFlowTask;
import org.neo4j.gds.paths.RelationshipCountProgressTaskFactory;
import org.neo4j.gds.paths.bellmanford.BellmanFordProgressTask;
import org.neo4j.gds.paths.delta.DeltaSteppingProgressTask;
import org.neo4j.gds.paths.traverse.bfs.BFSProgressTask;
import org.neo4j.gds.paths.traverse.dfs.DFSProgressTask;
import org.neo4j.gds.paths.yens.YensProgressTask;
import org.neo4j.gds.pricesteiner.PCSTProgressTrackerTaskCreator;
import org.neo4j.gds.steiner.SteinerTreeParameters;
import org.neo4j.gds.steiner.SteinerTreeProgressTask;
import org.neo4j.gds.traversal.RandomWalkCountingNodeVisitsProgressTaskFactory;
import org.neo4j.gds.traversal.RandomWalkProgressTask;

public final class PathFindingAlgorithmTasks {
    private PathFindingAlgorithmTasks() {}

    public static Task bellmanFord(Concurrency concurrency) {
        return BellmanFordProgressTask.create(concurrency);
    }

    public static Task bfs(Concurrency concurrency) {
        return BFSProgressTask.create(concurrency);
    }

    public static Task deltaStepping(Concurrency concurrency) {
        return DeltaSteppingProgressTask.create(concurrency);
    }

    public static Task dfs(Concurrency concurrency) {
        return DFSProgressTask.create(concurrency);
    }

    public static Task kSpanningTree(Graph graph, Concurrency concurrency) {
        return KSpanningTreeTask.create(concurrency, graph.relationshipCount());
    }

    public static Task longestPath(Graph graph, Concurrency concurrency) {
        return LongestPathTask.create(concurrency, graph.nodeCount());
    }

    public static Task maxFlow(Concurrency concurrency) {
        return MaxFlowTask.create(concurrency);
    }

    public static Task minCostMaxFlow(Concurrency concurrency) {
        return MinCostMaxFlowTask.create(concurrency);
    }

    public static Task randomWalk(Graph graph, Concurrency concurrency) {
        return RandomWalkProgressTask.create(graph, concurrency);
    }

    public static Task randomWalkCountingVisits(Graph graph, Concurrency concurrency) {
        return RandomWalkCountingNodeVisitsProgressTaskFactory.create(graph, concurrency);
    }

    public static Task pcst(Graph graph, Concurrency concurrency) {
        return PCSTProgressTrackerTaskCreator.progressTask(concurrency, graph.nodeCount(), graph.relationshipCount());
    }

    private static Task dijkstraVariant(Graph graph, Concurrency concurrency, AlgorithmLabel algorithmLabel) {
        return RelationshipCountProgressTaskFactory.create(algorithmLabel, concurrency, graph.relationshipCount());

    }

    public static Task aStar(Graph graph, Concurrency concurrency) {
        return dijkstraVariant(graph, concurrency, AlgorithmLabel.AStar);
    }

    public static Task dijkstra(Graph graph, Concurrency concurrency) {
        return dijkstraVariant(graph, concurrency, AlgorithmLabel.Dijkstra);
    }

    public static Task singleSourceDijkstra(Graph graph, Concurrency concurrency) {
        return dijkstraVariant(graph, concurrency, AlgorithmLabel.SingleSourceDijkstra);
    }

    public static Task spanningTree(Graph graph, Concurrency concurrency) {
        return RelationshipCountProgressTaskFactory.create(
            AlgorithmLabel.SpanningTree,
            concurrency,
            graph.relationshipCount()
        );
    }

    public static Task yens(Graph graph, Concurrency concurrency, int k) {
        return YensProgressTask.create(
            concurrency, graph.characteristics(),
            graph.nodeCount(),
            graph.relationshipCount(),
            k
        );
    }

    public static Task steinerTree(SteinerTreeParameters parameters, Graph graph) {
        return SteinerTreeProgressTask.create(parameters, graph.nodeCount());
    }

    public static Task topologicalSort(Graph graph, Concurrency concurrency) {
        return TopologicalSortTask.create(graph, concurrency);
    }
}
