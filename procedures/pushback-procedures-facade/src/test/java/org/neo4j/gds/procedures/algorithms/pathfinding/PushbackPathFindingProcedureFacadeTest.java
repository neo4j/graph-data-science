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
package org.neo4j.gds.procedures.algorithms.pathfinding;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.procedures.algorithms.pathfinding.mutate.PushbackPathFindingMutateProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.stats.PushbackPathFindingStatsProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.stream.PushbackPathFindingStreamProcedureFacade;
import org.neo4j.gds.procedures.algorithms.pathfinding.write.PushbackPathFindingWriteProcedureFacade;

import java.util.Map;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class PushbackPathFindingProcedureFacadeTest {

    private final String graphName = "g";
    private final Map<String, Object> config = Map.of();

    @Mock
    private PushbackPathFindingMutateProcedureFacade mutateFacadeMock;
    @Mock
    private PushbackPathFindingStatsProcedureFacade statsFacadeMock;
    @Mock
    private PushbackPathFindingStreamProcedureFacade streamFacadeMock;
    @Mock
    private PushbackPathFindingWriteProcedureFacade writeFacadeMock;

    private PushbackPathFindingProcedureFacade facade;

    @BeforeEach
    void setUp() {
        facade = new PushbackPathFindingProcedureFacade(
            mutateFacadeMock,
            statsFacadeMock,
            streamFacadeMock,
            writeFacadeMock
        );
    }

    @Nested
    class Mutate {
        @Test
        void bellmanFordMutate() {
            facade.bellmanFordMutate(graphName, config);
            verify(mutateFacadeMock).bellmanFord(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void breadthFirstSearchMutate() {
            facade.breadthFirstSearchMutate(graphName, config);
            verify(mutateFacadeMock).breadthFirstSearch(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void deltaSteppingMutate() {
            facade.deltaSteppingMutate(graphName, config);
            verify(mutateFacadeMock).deltaStepping(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void depthFirstSearchMutate() {
            facade.depthFirstSearchMutate(graphName, config);
            verify(mutateFacadeMock).depthFirstSearch(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void maxFlowMutate() {
            facade.maxFlowMutate(graphName, config);
            verify(mutateFacadeMock).maxFlow(graphName, config);
            verifyNoInteractions(streamFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void prizeCollectingSteinerTreeMutate() {
            facade.prizeCollectingSteinerTreeMutate(graphName, config);
            verify(mutateFacadeMock).prizeCollectingSteinerTree(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void randomWalkMutate() {
            facade.randomWalkMutate(graphName, config);
            verify(mutateFacadeMock).randomWalk(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void singlePairShortestPathAStarMutate() {
            facade.singlePairShortestPathAStarMutate(graphName, config);
            verify(mutateFacadeMock).singlePairShortestPathAStar(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void singlePairShortestPathDijkstraMutate() {
            facade.singlePairShortestPathDijkstraMutate(graphName, config);
            verify(mutateFacadeMock).singlePairShortestPathDijkstra(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void singlePairShortestPathYensMutate() {
            facade.singlePairShortestPathYensMutate(graphName, config);
            verify(mutateFacadeMock).singlePairShortestPathYens(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void singleSourceShortestPathDijkstraMutate() {
            facade.singleSourceShortestPathDijkstraMutate(graphName, config);
            verify(mutateFacadeMock).singleSourceShortestPathDijkstra(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void spanningTreeMutate() {
            facade.spanningTreeMutate(graphName, config);
            verify(mutateFacadeMock).spanningTree(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void steinerTreeMutate() {
            facade.steinerTreeMutate(graphName, config);
            verify(mutateFacadeMock).steinerTree(graphName, config);
            verifyNoInteractions(statsFacadeMock, streamFacadeMock, writeFacadeMock);
        }
    }

    @Nested
    class Stats {
        @Test
        void bellmanFordStats() {
            facade.bellmanFordStats(graphName, config);
            verify(statsFacadeMock).bellmanFord(graphName, config);
            verifyNoInteractions(mutateFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void breadthFirstSearchStats() {
            facade.breadthFirstSearchStats(graphName, config);
            verify(statsFacadeMock).breadthFirstSearch(graphName, config);
            verifyNoInteractions(mutateFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void deltaSteppingStats() {
            facade.deltaSteppingStats(graphName, config);
            verify(statsFacadeMock).deltaStepping(graphName, config);
            verifyNoInteractions(mutateFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void maxFlowStats() {
            facade.maxFlowStats(graphName, config);
            verify(statsFacadeMock).maxFlow(graphName, config);
            verifyNoInteractions(mutateFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void prizeCollectingSteinerTreeStats() {
            facade.prizeCollectingSteinerTreeStats(graphName, config);
            verify(statsFacadeMock).prizeCollectingSteinerTree(graphName, config);
            verifyNoInteractions(mutateFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void randomWalkStats() {
            facade.randomWalkStats(graphName, config);
            verify(statsFacadeMock).randomWalk(graphName, config);
            verifyNoInteractions(mutateFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void spanningTreeStats() {
            facade.spanningTreeStats(graphName, config);
            verify(statsFacadeMock).spanningTree(graphName, config);
            verifyNoInteractions(mutateFacadeMock, streamFacadeMock, writeFacadeMock);
        }

        @Test
        void steinerTreeStats() {
            facade.steinerTreeStats(graphName, config);
            verify(statsFacadeMock).steinerTree(graphName, config);
            verifyNoInteractions(mutateFacadeMock, streamFacadeMock, writeFacadeMock);
        }
    }

    @Nested
    class Stream {
        @Test
        void allShortestPathStream() {
            facade.allShortestPathStream(graphName, config);
            verify(streamFacadeMock).allShortestPaths(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void bellmanFordStream() {
            facade.bellmanFordStream(graphName, config);
            verify(streamFacadeMock).bellmanFord(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void breadthFirstSearchStream() {
            facade.breadthFirstSearchStream(graphName, config);
            verify(streamFacadeMock).breadthFirstSearch(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void deltaSteppingStream() {
            facade.deltaSteppingStream(graphName, config);
            verify(streamFacadeMock).deltaStepping(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void depthFirstSearchStream() {
            facade.depthFirstSearchStream(graphName, config);
            verify(streamFacadeMock).depthFirstSearch(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void longestPathStream() {
            facade.longestPathStream(graphName, config);
            verify(streamFacadeMock).longestPath(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void maxFlowStream() {
            facade.maxFlowStream(graphName, config);
            verify(streamFacadeMock).maxFlow(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void prizeCollectingSteinerTreeStream() {
            facade.prizeCollectingSteinerTreeStream(graphName, config);
            verify(streamFacadeMock).prizeCollectingSteinerTree(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void randomWalkStream() {
            facade.randomWalkStream(graphName, config);
            verify(streamFacadeMock).randomWalk(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void singlePairShortestPathAStarStream() {
            facade.singlePairShortestPathAStarStream(graphName, config);
            verify(streamFacadeMock).singlePairShortestPathAStar(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void singlePairShortestPathDijkstraStream() {
            facade.singlePairShortestPathDijkstraStream(graphName, config);
            verify(streamFacadeMock).singlePairShortestPathDijkstra(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void singlePairShortestPathYensStream() {
            facade.singlePairShortestPathYensStream(graphName, config);
            verify(streamFacadeMock).singlePairShortestPathYens(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void singleSourceShortestPathDijkstraStream() {
            facade.singleSourceShortestPathDijkstraStream(graphName, config);
            verify(streamFacadeMock).singleSourceShortestPathDijkstra(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void spanningTreeStream() {
            facade.spanningTreeStream(graphName, config);
            verify(streamFacadeMock).spanningTree(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void steinerTreeStream() {
            facade.steinerTreeStream(graphName, config);
            verify(streamFacadeMock).steinerTree(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }

        @Test
        void topologicalSortStream() {
            facade.topologicalSortStream(graphName, config);
            verify(streamFacadeMock).topologicalSort(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, writeFacadeMock);
        }
    }

    @Nested
    class Write {
        @Test
        void bellmanFordWrite() {
            facade.bellmanFordWrite(graphName, config);
            verify(writeFacadeMock).bellmanFord(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }

        @Test
        void deltaSteppingWrite() {
            facade.deltaSteppingWrite(graphName, config);
            verify(writeFacadeMock).deltaStepping(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }

        @Test
        void kSpanningTreeWrite() {
            facade.kSpanningTreeWrite(graphName, config);
            verify(writeFacadeMock).kSpanningTree(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }

        @Test
        void prizeCollectingSteinerTreeWrite() {
            facade.prizeCollectingSteinerTreeWrite(graphName, config);
            verify(writeFacadeMock).pcst(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }

        @Test
        void singlePairShortestPathAStarWrite() {
            facade.singlePairShortestPathAStarWrite(graphName, config);
            verify(writeFacadeMock).singlePairShortestPathAStar(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }

        @Test
        void singlePairShortestPathDijkstraWrite() {
            facade.singlePairShortestPathDijkstraWrite(graphName, config);
            verify(writeFacadeMock).singlePairShortestPathDijkstra(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }

        @Test
        void singlePairShortestPathYensWrite() {
            facade.singlePairShortestPathYensWrite(graphName, config);
            verify(writeFacadeMock).singlePairShortestPathYens(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }

        @Test
        void singleSourceShortestPathDijkstraWrite() {
            facade.singleSourceShortestPathDijkstraWrite(graphName, config);
            verify(writeFacadeMock).singleSourceShortestPathDijkstra(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }

        @Test
        void spanningTreeWrite() {
            facade.spanningTreeWrite(graphName, config);
            verify(writeFacadeMock).spanningTree(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }

        @Test
        void steinerTreeWrite() {
            facade.steinerTreeWrite(graphName, config);
            verify(writeFacadeMock).steinerTree(graphName, config);
            verifyNoInteractions(mutateFacadeMock, statsFacadeMock, streamFacadeMock);
        }
    }
}
