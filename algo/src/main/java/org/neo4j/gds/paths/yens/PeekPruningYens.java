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
package org.neo4j.gds.paths.yens;

import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public final class PeekPruningYens extends Algorithm<PathFindingResult> {
    private final Graph graph;
    private final long sourceNode;
    private final long targetNode;
    private final int k;
    private final Concurrency concurrency;
    private final ExecutorService executorService;

    public static PeekPruningYens sourceTarget(
        Graph graph,
        YensParameters parameters,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        return new PeekPruningYens(
            graph,
            graph.toMappedNodeId(parameters.sourceNode()),
            graph.toMappedNodeId( parameters.targetNode()),
            parameters.k(),
            parameters.concurrency(),
            executorService,
            progressTracker,
            terminationFlag
        );
    }

    public PeekPruningYens(
        Graph graph,
        long sourceNode,
        long targetNode,
        int k,
        Concurrency concurrency,
        ExecutorService executorService,
        ProgressTracker progressTracker,
        TerminationFlag terminationFlag
    ) {
        super(progressTracker);
        this.graph = graph;
        this.terminationFlag = terminationFlag;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.k = k;
        this.concurrency = concurrency;
        this.executorService = executorService;
    }

    @Override
    public PathFindingResult compute() {
        progressTracker.beginSubTask("Peek pruning");
        var nodeCount = graph.nodeCount();
        var paths = PeekPruning.pathsAndReachability(graph, sourceNode, targetNode, PeekPruning.deltaStep(concurrency, executorService, terminationFlag));
        if (paths.reachable().cardinality() == 0) {
            progressTracker.endSubTask("Peek pruning");
            return new PathFindingResult(Stream.empty());
        }
        var combinedPaths = PeekPruning.sortedCombinedPathCosts(nodeCount, paths.reachable().cardinality(), paths.reachable()::get, paths.forward()::distance, paths.backward()::distance);
        var validCosts = PeekPruning.validPathCosts(k, nodeCount, sourceNode, targetNode, combinedPaths, paths.forward()::predecessor, paths.backward()::predecessor);
        // make the cutoff just slightly larger than computed, to avoid floating point errors
        double cutoff = 1.000001 * validCosts[validCosts.length-1];
        var nodeIncluded = PeekPruning.nodeFilter(nodeCount, combinedPaths, cutoff);
        var pruned = PeekPruning.createPrunedGraph(graph, sourceNode, cutoff, paths.forward()::distance, paths.backward()::distance, nodeIncluded::get, concurrency);
        progressTracker.endSubTask("Peek pruning");

        return PeekPruning.mapToOriginalGraph(
            pruned,
            Yens.sourceTarget(
                pruned,
                new YensParameters(
                    sourceNode,
                    targetNode,
                    k,
                    concurrency),
                progressTracker,
                terminationFlag).compute());
    }
}
