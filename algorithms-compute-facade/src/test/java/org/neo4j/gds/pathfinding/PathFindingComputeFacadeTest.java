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
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.IterativeTask;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.dag.longestPath.DagLongestPathParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortParameters;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.kspanningtree.KSpanningTreeParameters;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.paths.astar.AStarParameters;
import org.neo4j.gds.paths.bellmanford.BellmanFordParameters;
import org.neo4j.gds.paths.delta.DeltaSteppingParameters;
import org.neo4j.gds.paths.dijkstra.DijkstraSingleSourceParameters;
import org.neo4j.gds.paths.dijkstra.DijkstraSourceTargetParameters;
import org.neo4j.gds.paths.yens.YensParameters;
import org.neo4j.gds.pcst.PCSTParameters;
import org.neo4j.gds.spanningtree.PrimOperators;
import org.neo4j.gds.spanningtree.SpanningTreeParameters;
import org.neo4j.gds.steiner.SteinerTreeParameters;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.RandomWalkParameters;
import org.neo4j.gds.traversal.TraversalParameters;
import org.neo4j.gds.traversal.WalkParameters;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@GdlExtension
class PathFindingComputeFacadeTest {

    @Mock(strictness = Mock.Strictness.LENIENT)
    private ProgressTrackerFactory progressTrackerFactoryMock;
    @Mock
    private ProgressTracker progressTrackerMock;

    @Mock
    private JobId jobIdMock;

    @Mock
    private Log logMock;

    @GdlGraph
    private static final String GDL = """
        (a:Node { prize: 1.0 })-[:REL]->(b:Node { prize: 2.0 }),
        (b)-[:REL]->(c:Node { prize: 3.0 }),
        (a)-[:REL]->(c)
        """;

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;
    private PathFindingComputeFacade facade;

    @BeforeEach
    void setUp() {
        when(progressTrackerFactoryMock.nullTracker())
            .thenReturn(ProgressTracker.NULL_TRACKER);
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(progressTrackerMock);

        facade = new PathFindingComputeFacade(
            new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), logMock),
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            progressTrackerFactoryMock
        );
    }

    @Test
    void allShortestPaths() {
        var future = facade.allShortestPaths(
            graph,
            new AllShortestPathsParameters(new Concurrency(4), false),
            jobIdMock
        );

        var results = future.join();
        long a = idFunction.of("a");
        long c = idFunction.of("c");

        assertThat(results.result()).isNotEmpty()
            .anySatisfy(r -> assertThat(r.sourceNodeId()).isEqualTo(a))
            .anySatisfy(r -> assertThat(r.targetNodeId()).isEqualTo(c));
    }

    @Test
    void bellmanFord() {
        var future = facade.bellmanFord(
            graph,
            new BellmanFordParameters(
                idFunction.of("a"),
                false,
                true,
                new Concurrency(4)
            ),
            jobIdMock,
            false
        );

        var result = future.join();
        assertThat(result.result()).isNotNull();

        verify(progressTrackerFactoryMock, times(1))
            .create(isA(IterativeTask.class), eq(jobIdMock), eq(new Concurrency(4)), eq(false));
        verifyNoMoreInteractions(progressTrackerFactoryMock);
    }


    @Test
    void breadthFirstSearch() {
        var future = facade.breadthFirstSearch(
            graph,
            new TraversalParameters(
                idFunction.of("a"),
                List.of(idFunction.of("c")),
                3L,
                new Concurrency(2)
            ),
            jobIdMock,
            false
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void deltaStepping() {
        var future = facade.deltaStepping(
            graph,
            DeltaSteppingParameters.withDefaultDelta(
                idFunction.of("a"),
                new Concurrency(2)
            ),
            jobIdMock,
            false
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void depthFirstSearch() {
        var future = facade.depthFirstSearch(
            graph,
            new TraversalParameters(
                idFunction.of("a"),
                List.of(idFunction.of("c")),
                3L,
                new Concurrency(2)
            ),
            jobIdMock,
            false
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void kSpanningTree() {
        var future = facade.kSpanningTree(
            graph,
            new KSpanningTreeParameters(
                PrimOperators.MIN_OPERATOR,
                idFunction.of("a"),
                2L,
                new Concurrency(2)
            ),
            jobIdMock,
            false
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void longestPath() {
        var future = facade.longestPath(
            graph,
            new DagLongestPathParameters(
                new Concurrency(2)
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void randomWalk() {
        var future = facade.randomWalk(
            graph,
            new RandomWalkParameters(
                List.of(idFunction.of("a")),
                WalkParameters.DEFAULTS,
                1000,
                Optional.of(19L),
                new Concurrency(2)
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void randomWalkCountingNodeVisits() {
        var future = facade.randomWalkCountingNodeVisits(
            graph,
            new RandomWalkParameters(
                List.of(idFunction.of("a")),
                WalkParameters.DEFAULTS,
                1000,
                Optional.of(19L),
                new Concurrency(2)
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void pcst() {
        var future = facade.pcst(
            graph,
            new PCSTParameters(
                "prize",
                new Concurrency(2)
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void singlePairShortestPathAStar() {
        var future = facade.singlePairShortestPathAStar(
            graph,
            new AStarParameters(
                "prize",
                "prize",
                idFunction.of("a"),
                idFunction.of("c"),
                new Concurrency(2)
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void singlePairShortestPathDijkstra() {
        var future = facade.singlePairShortestPathDijkstra(
            graph,
            new DijkstraSourceTargetParameters(
                idFunction.of("a"),
                List.of(idFunction.of("c")),
                new Concurrency(2)
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void singlePairShortestPathYens() {
        var future = facade.singlePairShortestPathYens(
            graph,
            new YensParameters(
                idFunction.of("a"),
                idFunction.of("c"),
                1,
                new Concurrency(2)
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void singleSourceShortestPathDijkstra() {
        var future = facade.singleSourceShortestPathDijkstra(
            graph,
            new DijkstraSingleSourceParameters(idFunction.of("a")),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void spanningTree() {
        var future = facade.spanningTree(
            graph,
            new SpanningTreeParameters(
                PrimOperators.MIN_OPERATOR,
                idFunction.of("a")
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void steinerTree() {
        var future = facade.steinerTree(
            graph,
            new SteinerTreeParameters(
                new Concurrency(4),
                idFunction.of("a"),
                List.of(idFunction.of("c")),
                2.0,
                false
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void topologicalSort() {
        var future = facade.topologicalSort(
            graph,
            new TopologicalSortParameters(
                false,
                new Concurrency(4)
            ),
            jobIdMock,
            true
        );
        assertThat(future.join()).isNotNull();
    }

}
