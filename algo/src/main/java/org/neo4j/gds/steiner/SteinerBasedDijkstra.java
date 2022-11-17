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
package org.neo4j.gds.steiner;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.DoubleArrayDeque;
import com.carrotsearch.hppc.LongArrayDeque;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.core.utils.paged.HugeDoubleArray;
import org.neo4j.gds.core.utils.paged.HugeLongLongMap;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.queue.HugeLongPriorityQueue;
import org.neo4j.gds.paths.ImmutablePathResult;
import org.neo4j.gds.paths.PathResult;
import org.neo4j.gds.paths.dijkstra.DijkstraResult;

import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

/*
 * the idea of this Dijkstra modification is to provide an efficient implementation to the following heuristic:
 *
 * while there exists a terminal for which a path form the source has not been found yet
 *              1. Find the shortest path in the current graph to any unvisited terminal
 *              2. Modify the graph such that all edges in the discovered path have weight equal to zero.
 *
 * We do implicit zeroing by keeping an array of vertices which are accessible from root with zero cost.
 * Also, when a path to terminal has been found, we do not start a shortest path search from scratch.
 * We instead continue from where we left off.
 */
class SteinerBasedDijkstra extends Algorithm<DijkstraResult> {
    private final Graph graph;
    private long sourceNode;
    private final HugeLongPriorityQueue queue;
    private final HugeLongLongMap predecessors;
    private final BitSet visited;
    private long pathIndex;
    private final BitSet isTerminal;
    private final LongAdder metTerminals;
    private final HugeDoubleArray distances;

    SteinerBasedDijkstra(
        Graph graph,
        long sourceNode,
        BitSet isTerminal
    ) {
        super(ProgressTracker.NULL_TRACKER);
        this.graph = graph;
        this.sourceNode = sourceNode;
        this.queue = HugeLongPriorityQueue.min(graph.nodeCount());
        this.predecessors = new HugeLongLongMap();
        this.visited = new BitSet();
        this.pathIndex = 0L;
        this.isTerminal = isTerminal;
        this.metTerminals = new LongAdder();
        this.distances = HugeDoubleArray.newArray(graph.nodeCount());
    }

    public DijkstraResult compute() {

        queue.add(sourceNode, 0.0);

        var pathResultBuilder = ImmutablePathResult.builder()
            .sourceNode(sourceNode);

        //this bitset tracks which vertices have been "merged" with source
        //i.e.,  those vertices for which their path to the root has been modified to zero cost
        //in essence it's as if they have been merged with the source into a ''supernode''
        //the structure is modified every time a path to a terminal from source is found
        //by setting to "1" all un-merged vertices in the path.
        var mergedWithSource = new BitSet(graph.nodeCount());
        mergedWithSource.set(sourceNode);

        var paths = Stream
            .generate(() -> next(mergedWithSource, pathResultBuilder))
            .takeWhile(pathResult -> pathResult != PathResult.EMPTY);

        return new DijkstraResult(paths);
    }

    @Override
    public void release() {

    }

    private void specialRelaxNode(long nodeId, BitSet mergedWithSource) {
        //this is a modified node relaxation to fit our needs

        //normally, dijkstra relaxation for a node  u works as follows:
        //for  v : neighbor (u) if  !visited(v)  update-in-queue

        //this dijkstra modification, however, can update v even if v was marked as visited.
        //this is a consequence of not restarting dijkstra from scratch but continuing from where we left of
        //the neighbors of a  freshly merged node u will need to have their score updated
        //because   the cost of u->v is not   distance(u)+ cost(u->v) anymore but  only cost(u->v)
        //since u is assumed to be merged to the source.
        // So even if v has been marked as visited, it makes sense to reprocess it.
        //Of course  v's own children might need to be reupdated due to v's potential new distance etc.

        //if this situation happens, we simply flip the visited state of a node to unvisited

        double cost = distances.get(nodeId);
        graph.forEachRelationship(
            nodeId,
            1.0D,
            (source, target, weight) -> {
                if (visited.get(target)) { //let's see if it makes sense to flip it
                    reExamineVisitedState(target, mergedWithSource.get(target), weight + cost);
                }
                if (!visited.get(target)) { //if unvisited, try to update score
                    updateCost(source, target, weight + cost);
                }
                return true;
            }
        );
    }

    private void reExamineVisitedState(long target, boolean targetIsMergedWithSource, double cost) {
        //if it is merged with the source, we must not touch it
        //otherwise, if we can improve on its current score let's go for it!
        if (!targetIsMergedWithSource && distances.get(target) > cost) {
            visited.flip(target); //before it was true, now it becomes false again
        }
    }

    private void mergeNodesOnPathToSource(long nodeId, BitSet mergedWithSource) {
        long currentId = nodeId;
        //while not meeting merged nodes, add the current path node to the merge set
        //if the parent i merged, then it's path to the source has already been zeroed,
        //hence we can stop
        while (!mergedWithSource.getAndSet(currentId)) {
            if (currentId != nodeId) { //this is just to avoid processing "nodeId" twice
                queue.set(currentId, 0); //add the node to the queue with cost 0
            }
            distances.set(currentId, 0);

            currentId = predecessors.getOrDefault(currentId, -1);
            if (currentId == sourceNode || currentId == -1) {
                break;
            }
        }
    }

    private PathResult next(
        BitSet mergedWithSource,
        ImmutablePathResult.Builder pathResultBuilder
    ) {

        long numberOfTerminals = isTerminal.cardinality();
        while (!queue.isEmpty() && numberOfTerminals > metTerminals.longValue()) {
            var node = queue.pop();
            var cost = queue.cost(node);
            distances.set(node, cost);
            visited.set(node);
            if (isTerminal.get(node)) { //if we have found a terminal

                metTerminals.increment();

                var pathResult = pathResult(node, pathResultBuilder, mergedWithSource);

                if (metTerminals.longValue() < numberOfTerminals) { //this just avoids extra work
                    mergeNodesOnPathToSource(node, mergedWithSource); ///update the merged bitset
                    specialRelaxNode(node, mergedWithSource); //examine all neighbors of node
                }
                return pathResult;

            } else {
                specialRelaxNode(node, mergedWithSource); //examine all neighbors of node
                //we do not return anything, because we only care about paths ending in terminals
            }
        }
        return PathResult.EMPTY;
    }

    private void updateCost(long source, long target, double newCost) {
        boolean shouldUpdate = !queue.containsElement(target) || newCost < queue.cost(target);
        if (shouldUpdate) {
            queue.set(target, newCost);
            predecessors.put(target, source);
        }
    }

    private static final long[] EMPTY_ARRAY = new long[0];

    //we generate the path only until the first discovered merged node
    private PathResult pathResult(
        long target,
        ImmutablePathResult.Builder pathResultBuilder,
        BitSet mergedWithSource
    ) {
        var pathNodeIds = new LongArrayDeque();
        var costs = new DoubleArrayDeque();

        var pathStart = this.sourceNode;
        var lastNode = target;

        while (true) {
            pathNodeIds.addFirst(lastNode); //node is always added
            if (mergedWithSource.get(lastNode)) {
                break;
            }
            costs.addFirst(queue.cost(lastNode)); //cost is added except the very last one

            lastNode = this.predecessors.getOrDefault(lastNode, pathStart);

        }

        return pathResultBuilder
            .index(pathIndex++)
            .targetNode(target)
            .nodeIds(pathNodeIds.toArray())
            .relationshipIds(EMPTY_ARRAY)
            .costs(costs.toArray())
            .build();
    }


}
