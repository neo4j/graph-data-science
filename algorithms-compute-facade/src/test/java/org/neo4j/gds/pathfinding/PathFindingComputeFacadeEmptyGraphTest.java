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
package org.neo4j.gds.pathfinding;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.dag.longestPath.DagLongestPathParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.kspanningtree.KSpanningTreeParameters;
import org.neo4j.gds.paths.astar.AStarParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFordParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFordResult;
import org.neo4j.gds.paths.delta.DeltaSteppingParameters;
import org.neo4j.gds.paths.dijkstra.DijkstraSingleSourceParameters;
import org.neo4j.gds.paths.dijkstra.DijkstraSourceTargetParameters;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.paths.yens.YensParameters;
import org.neo4j.gds.pcst.PCSTParameters;
import org.neo4j.gds.pricesteiner.PrizeSteinerTreeResult;
import org.neo4j.gds.spanningtree.SpanningTree;
import org.neo4j.gds.spanningtree.SpanningTreeParameters;
import org.neo4j.gds.steiner.SteinerTreeParameters;
import org.neo4j.gds.steiner.SteinerTreeResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.RandomWalkParameters;
import org.neo4j.gds.traversal.TraversalParameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PathFindingComputeFacadeEmptyGraphTest {
    @Mock
    private Graph graph;

    @Mock
    private ProgressTrackerFactory progressTrackerFactoryMock;

    @Mock
    private AsyncAlgorithmCaller algorithmCallerMock;

    @Mock
    private JobId jobIdMock;

    private PathFindingComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(graph.isEmpty()).thenReturn(true);
        facade = new PathFindingComputeFacade(
            algorithmCallerMock,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            progressTrackerFactoryMock
        );
    }

    @Test
    void allShortestPaths() {
        var future = facade.allShortestPaths(
            graph,
            mock(AllShortestPathsParameters.class),
            jobIdMock
        );
        var result = future.join();
        assertThat(result).isEmpty();

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void bellmanFord() {
        var future = facade.bellmanFord(
            graph,
            mock(BellmanFordParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(BellmanFordResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void breadthFirstSearch() {
        var future = facade.breadthFirstSearch(
            graph,
            mock(TraversalParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull();
        assertThat(result.size()).isZero();

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void deltaStepping() {
        var future = facade.deltaStepping(
            graph,
            mock(DeltaSteppingParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(PathFindingResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void depthFirstSearch() {
        var future = facade.depthFirstSearch(
            graph,
            mock(TraversalParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull();
        assertThat(result.size()).isZero();

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void kSpanningTree() {
        var future = facade.kSpanningTree(
            graph,
            mock(KSpanningTreeParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(SpanningTree.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void longestPath() {
        var future = facade.longestPath(
            graph,
            mock(DagLongestPathParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(PathFindingResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void randomWalk() {
        var future = facade.randomWalk(
            graph,
            mock(RandomWalkParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEmpty();

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);

    }

    @Test
    void randomWalkCountingNodeVisits() {
        var future = facade.randomWalkCountingNodeVisits(
            graph,
            mock(RandomWalkParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull();
        assertThat(result.size()).isZero();

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void pcst() {
        var future = facade.pcst(
            graph,
            mock(PCSTParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(PrizeSteinerTreeResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void singlePairShortestPathAStar() {
        var future = facade.singlePairShortestPathAStar(
            graph,
            mock(AStarParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(PathFindingResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void singlePairShortestPathDijkstra() {
        var future = facade.singlePairShortestPathDijkstra(
            graph,
            mock(DijkstraSourceTargetParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(PathFindingResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void singlePairShortestPathYens() {
        var future = facade.singlePairShortestPathYens(
            graph,
            mock(YensParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(PathFindingResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void singleSourceShortestPathDijkstra() {
        var future = facade.singleSourceShortestPathDijkstra(
            graph,
            mock(DijkstraSingleSourceParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(PathFindingResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void spanningTree() {
        var future = facade.spanningTree(
            graph,
            mock(SpanningTreeParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(SpanningTree.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void steinerTree() {
        var future = facade.steinerTree(
            graph,
            mock(SteinerTreeParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(SteinerTreeResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

    @Test
    void topologicalSort() {
        var future = facade.topologicalSort(
            graph,
            mock(TopologicalSortParameters.class),
            jobIdMock,
            false
        );
        var result = future.join();

        assertThat(result).isNotNull().isEqualTo(TopologicalSortResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

}
