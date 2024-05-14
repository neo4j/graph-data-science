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
package org.neo4j.gds.ml.pipeline;

import org.neo4j.gds.applications.algorithms.metadata.Algorithm;
import org.neo4j.gds.ml.pipeline.stubs.BellmanFordStub;
import org.neo4j.gds.ml.pipeline.stubs.BetaClosenessCentralityStub;
import org.neo4j.gds.ml.pipeline.stubs.BetweennessCentralityStub;
import org.neo4j.gds.ml.pipeline.stubs.BreadthFirstSearchStub;
import org.neo4j.gds.ml.pipeline.stubs.ClosenessCentralityStub;
import org.neo4j.gds.ml.pipeline.stubs.DepthFirstSearchStub;
import org.neo4j.gds.ml.pipeline.stubs.FilteredKnnStub;
import org.neo4j.gds.ml.pipeline.stubs.FilteredNodeSimilarityStub;
import org.neo4j.gds.ml.pipeline.stubs.KnnStub;
import org.neo4j.gds.ml.pipeline.stubs.NodeSimilarityStub;
import org.neo4j.gds.ml.pipeline.stubs.SinglePairShortestPathAStarStub;
import org.neo4j.gds.ml.pipeline.stubs.SinglePairShortestPathDijkstraStub;
import org.neo4j.gds.ml.pipeline.stubs.SinglePairShortestPathYensStub;
import org.neo4j.gds.ml.pipeline.stubs.SingleSourceShortestPathDeltaStub;
import org.neo4j.gds.ml.pipeline.stubs.SingleSourceShortestPathDijkstraStub;
import org.neo4j.gds.ml.pipeline.stubs.SpanningTreeStub;
import org.neo4j.gds.ml.pipeline.stubs.SteinerTreeStub;

/**
 * :flag-au:
 * The mapping of procedure name -> algorithm identifier -> stub
 * NB: only for algorithms that have a mutate mode - otherwise they could not form part of a pipeline
 */
class StubbyHolder {
    /**
     * @return a handy stub, or null if the algorithm does not have a mutate mode
     */
    Stub get(Algorithm algorithm) {
        return switch (algorithm) {
            case AllShortestPaths -> null;
            case AStar -> new SinglePairShortestPathAStarStub();
            case BellmanFord -> new BellmanFordStub();
            case BetaClosenessCentrality -> new BetaClosenessCentralityStub();
            case BetweennessCentrality -> new BetweennessCentralityStub();
            case BFS -> new BreadthFirstSearchStub();
            case ClosenessCentrality -> new ClosenessCentralityStub();
            case DeltaStepping -> new SingleSourceShortestPathDeltaStub();
            case DFS -> new DepthFirstSearchStub();
            case Dijkstra -> new SinglePairShortestPathDijkstraStub();
            case FilteredKNN -> new FilteredKnnStub();
            case FilteredNodeSimilarity -> new FilteredNodeSimilarityStub();
            case KNN -> new KnnStub();
            case KSpanningTree -> null;
            case LongestPath -> null;
            case NodeSimilarity -> new NodeSimilarityStub();
            case RandomWalk -> null;
            case SingleSourceDijkstra -> new SingleSourceShortestPathDijkstraStub();
            case SpanningTree -> new SpanningTreeStub();
            case SteinerTree -> new SteinerTreeStub();
            case TopologicalSort -> null;
            case Yens -> new SinglePairShortestPathYensStub();
        };
    }
}
