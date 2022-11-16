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
        //the structure is modified every time a path to a termninal from source is found
        //by  setting to "1" all un-merged vertices in the path.
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
          //a node is allowed to add to the priority queue, already visited nodes
          //provided that they have not been merged to the root
          //in that set rather than visited being a 0/1  "boolean" array
          //consider visited as an array from 0...|terminals| where during iteration j
          // visited[i] = j  would be equal to "true" and visited[i] < j would be equal to "false"
          // and visited[i] gets sets to j, when its visited in the j-th iteration
          double cost = distances.get(nodeId);
          graph.forEachRelationship(
              nodeId,
              1.0D,
              (source, target, weight) -> {
                  if (visited.get(target)) {
                      reexamineVisitedState(target, mergedWithSource.get(target), weight + cost);
                  }
                  if (!visited.get(target)) {
                      updateCost(source, target, weight + cost);
                  }
                  return true;
              }
          );
      }

      private void reexamineVisitedState(long target, boolean targetIsMergedWithSource, double cost) {
          if (!targetIsMergedWithSource && distances.get(target) > cost) {
              visited.flip(target); //before it was true, now it becomes false again
          }
      }

      private void mergeNodesOnPathToSource(long nodeId, BitSet mergedWithSource) {
          long currentId = nodeId;
          //while i am not meeting merged nodes, add the current path node to the merge set
          //if the parent i merged, then it's path to the source has already been zeroed,
          //hence we can stop
          while (!mergedWithSource.getAndSet(currentId)) {
              if (currentId != nodeId) { //this is just to avoid processing "nodeId" twice
                  queue.add(currentId, 0); //add the node to the queue with cost 0
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

                  var pathResult = pathResult(node, pathResultBuilder);

                  if (metTerminals.longValue() < numberOfTerminals) { //this is just optimization
                      mergeNodesOnPathToSource(node, mergedWithSource);
                      specialRelaxNode(node, mergedWithSource);
                  }
                  return pathResult;

              } else {
                  specialRelaxNode(node, mergedWithSource);
              }
        }
        return PathResult.EMPTY;
    }

    private void updateCost(long source, long target, double newCost) {
        boolean shouldUpdate= !queue.containsElement(target) ||newCost < queue.cost(target);
        if (shouldUpdate) {
            queue.add(target, newCost);
            predecessors.put(target, source);
        }
    }

    private static final long[] EMPTY_ARRAY = new long[0];
    private PathResult pathResult(long target, ImmutablePathResult.Builder pathResultBuilder) {
        var pathNodeIds = new LongArrayDeque();
        var costs = new DoubleArrayDeque();

        var pathStart = this.sourceNode;
        var lastNode = target;

        while (true) {
            pathNodeIds.addFirst(lastNode);
            costs.addFirst(queue.cost(lastNode));

            if (lastNode == pathStart) {
                break;
            }

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
