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
import org.neo4j.gds.core.utils.progress.tasks.Task;
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


    public Task bellmanFord(){
        return BellmanFordProgressTask.create();
    }

    public Task bfs(){
        return BFSProgressTask.create();
    }

    public Task deltaStepping(){
        return DeltaSteppingProgressTask.create();
    }

    public Task dfs(){
        return DFSProgressTask.create();
    }

    public Task kSpanningTree(Graph graph){
        return KSpanningTreeTask.create(graph.relationshipCount());
    }

    public Task longestPath(Graph graph){
        return LongestPathTask.create(graph.nodeCount());
    }

    public Task maxFlow(){
        return MaxFlowTask.create();
    }

    public Task minCostMaxFlow(){
        return MinCostMaxFlowTask.create();
    }

    public Task randomWalk(Graph graph){
        return RandomWalkProgressTask.create(graph);
    }

    public Task randomWalkCountingVisits(Graph graph){
        return RandomWalkCountingNodeVisitsProgressTaskFactory.create(graph);
    }

    public Task pcst(Graph graph){
        return PCSTProgressTrackerTaskCreator.progressTask(graph.nodeCount(), graph.relationshipCount());
    }

    private Task dijkstraVariant(AlgorithmLabel algorithmLabel,Graph graph){
        return RelationshipCountProgressTaskFactory.create(algorithmLabel, graph.relationshipCount());

    }
    public Task aStar(Graph graph){
        return dijkstraVariant(AlgorithmLabel.AStar,graph);
    }

    public Task dijkstra(Graph graph){
        return dijkstraVariant(AlgorithmLabel.Dijkstra, graph);
    }

    public Task singleSourceDijkstra(Graph graph){
        return dijkstraVariant(AlgorithmLabel.SingleSourceDijkstra, graph);
    }

    public Task spanningTree(Graph graph){
        return RelationshipCountProgressTaskFactory.create(AlgorithmLabel.SpanningTree, graph.relationshipCount());
    }

    public Task yens(Graph graph,int k){
       return YensProgressTask.create(
            graph.characteristics(),
            graph.nodeCount(),
            graph.relationshipCount(),
            k
        );
    }

    public Task steinerTree(SteinerTreeParameters parameters, Graph graph){
       return SteinerTreeProgressTask.create(parameters, graph.nodeCount());
    }

    public Task topologicalSort(Graph graph){
        return TopologicalSortTask.create(graph);
    }
}
