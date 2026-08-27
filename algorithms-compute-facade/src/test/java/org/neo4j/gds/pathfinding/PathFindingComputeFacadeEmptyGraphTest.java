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
import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.JobId;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.progress.tracking.ProgressTrackerFactory;
import org.neo4j.gds.dag.longestPath.DagLongestPathParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortParameters;
import org.neo4j.gds.dag.topologicalsort.TopologicalSortResult;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.paths.dijkstra.PathFindingResult;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.RandomWalkParameters;
import org.neo4j.gds.traversal.TraversalParameters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.SET;
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
            Log.noOpLog(),
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
        assertThat(result.result()).isEmpty();

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
        assertThat(result.result().size()).isZero();

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
        assertThat(result.result().size()).isZero();

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

        assertThat(result.result())
            .isNotNull()
            .extracting(PathFindingResult::pathSet)
            .asInstanceOf(SET)
            .isEmpty();

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

        assertThat(result.result()).isNotNull().isEmpty();

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

        assertThat(result.result()).isNotNull().isEqualTo(TopologicalSortResult.EMPTY);

        verifyNoInteractions(progressTrackerFactoryMock);
        verifyNoInteractions(algorithmCallerMock);
    }

}
