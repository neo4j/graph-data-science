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
import org.neo4j.gds.TestGraph;
import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.progress.tracking.ProgressTracker;
import org.neo4j.gds.dag.longestPath.DagLongestPathParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortParameters;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.Log;
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
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(progressTrackerMock);

        facade = new PathFindingComputeFacade(
            Log.noOpLog(),
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
