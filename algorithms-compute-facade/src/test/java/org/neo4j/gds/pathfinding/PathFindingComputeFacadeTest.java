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
import org.neo4j.gds.GraphParameters;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.ProgressTrackerFactory;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.allshortestpaths.AllShortestPathsParameters;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.async.AsyncAlgorithmCaller;
import org.neo4j.gds.core.concurrency.Concurrency;
import org.neo4j.gds.core.concurrency.DefaultPool;
import org.neo4j.gds.core.loading.GraphResources;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.progress.JobId;
import org.neo4j.gds.core.utils.progress.tasks.IterativeTask;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.kspanningtree.KSpanningTreeParameters;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.paths.bellmanford.BellmanFordParameters;
import org.neo4j.gds.paths.delta.DeltaSteppingParameters;
import org.neo4j.gds.spanningtree.PrimOperators;
import org.neo4j.gds.termination.TerminationFlag;
import org.neo4j.gds.traversal.TraversalParameters;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@GdlExtension
class PathFindingComputeFacadeTest {

    private static final GraphParameters GRAPH_PARAMETERS = new GraphParameters(
        List.of(NodeLabel.of("Node")),
        List.of(RelationshipType.of("REL")),
        true,
        Optional.empty()
    );

    @Mock
    private GraphStoreCatalogService catalogServiceMock;

    @GdlGraph
    private static final String GDL = """
        (a:Node)-[:REL]->(b:Node),
        (b)-[:REL]->(c:Node),
        (a)-[:REL]->(c)
        """;

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setUp() {
        when(catalogServiceMock.fetchGraphResources(
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any()
        )).thenReturn(new GraphResources(mock(GraphStore.class), graph, mock(ResultStore.class)));
    }

    @Test
    void allShortestPaths() {

        var algorithmCaller = new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), mock(Log.class));

        var progressTrackerFactoryMock = mock(ProgressTrackerFactory.class);
        when(progressTrackerFactoryMock.nullTracker()).thenReturn(ProgressTracker.NULL_TRACKER);

        var facade = new PathFindingComputeFacade(
            catalogServiceMock,
            algorithmCaller,
            mock(User.class),
            DatabaseId.DEFAULT,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            progressTrackerFactoryMock
        );

        var future = facade.allShortestPaths(
            new GraphName("foo"),
            GRAPH_PARAMETERS,
            Optional.empty(),
            new AllShortestPathsParameters(new Concurrency(4), false),
            mock(JobId.class)
        );

        var results = future.join();
        long a = idFunction.of("a");
        long c = idFunction.of("c");

        assertThat(results).isNotEmpty()
            .anySatisfy(r -> assertThat(r.sourceNodeId()).isEqualTo(a))
            .anySatisfy(r -> assertThat(r.targetNodeId()).isEqualTo(c));
    }

    @Test
    void bellmanFord() {
        var jobIdMock = mock(JobId.class);
        var algorithmCaller = new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), mock(Log.class));

        var progressTrackerFactoryMock = mock(ProgressTrackerFactory.class);
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(ProgressTracker.NULL_TRACKER);

        var facade = new PathFindingComputeFacade(
            catalogServiceMock,
            algorithmCaller,
            mock(User.class),
            DatabaseId.DEFAULT,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            progressTrackerFactoryMock
        );

        var future = facade.bellmanFord(
            new GraphName("foo"),
            GRAPH_PARAMETERS,
            Optional.empty(),
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
        assertThat(result).isNotNull();

        verify(progressTrackerFactoryMock, times(1))
            .create(isA(IterativeTask.class), eq(jobIdMock), eq(new Concurrency(4)), eq(false));
        verifyNoMoreInteractions(progressTrackerFactoryMock);
    }


    @Test
    void breadthFirstSearch() {

        var algorithmCaller = new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), mock(Log.class));
        var progressTrackerFactoryMock = mock(ProgressTrackerFactory.class);
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(ProgressTracker.NULL_TRACKER);

        var facade = new PathFindingComputeFacade(
            catalogServiceMock,
            algorithmCaller,
            mock(User.class),
            DatabaseId.DEFAULT,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            progressTrackerFactoryMock
        );

        var future = facade.breadthFirstSearch(
            new GraphName("foo"),
            GRAPH_PARAMETERS,
            new TraversalParameters(
                idFunction.of("a"),
                List.of(idFunction.of("c")),
                3L,
                new Concurrency(2)
            ),
            mock(JobId.class),
            false
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void deltaStepping() {
        var algorithmCaller = new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), mock(Log.class));
        var progressTrackerFactoryMock = mock(ProgressTrackerFactory.class);
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(ProgressTracker.NULL_TRACKER);

        var facade = new PathFindingComputeFacade(
            catalogServiceMock,
            algorithmCaller,
            mock(User.class),
            DatabaseId.DEFAULT,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            progressTrackerFactoryMock
        );

        var future = facade.deltaStepping(
            new GraphName("foo"),
            GRAPH_PARAMETERS,
            Optional.empty(),
            DeltaSteppingParameters.withDefaultDelta(
                idFunction.of("a"),
                new Concurrency(2)
            ),
            mock(JobId.class),
            false
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void depthFirstSearch() {
        var algorithmCaller = new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), mock(Log.class));
        var progressTrackerFactoryMock = mock(ProgressTrackerFactory.class);
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(mock(org.neo4j.gds.core.utils.progress.tasks.ProgressTracker.class));

        var facade = new PathFindingComputeFacade(
            catalogServiceMock,
            algorithmCaller,
            mock(User.class),
            DatabaseId.DEFAULT,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            progressTrackerFactoryMock
        );

        var future = facade.depthFirstSearch(
            new GraphName("foo"),
            GRAPH_PARAMETERS,
            new TraversalParameters(
                idFunction.of("a"),
                List.of(idFunction.of("c")),
                3L,
                new Concurrency(2)
            ),
            mock(JobId.class),
            false
        );
        assertThat(future.join()).isNotNull();
    }

    @Test
    void kSpanningTree() {

        var algorithmCaller = new AsyncAlgorithmCaller(Executors.newSingleThreadExecutor(), mock(Log.class));
        var progressTrackerFactoryMock = mock(ProgressTrackerFactory.class);
        when(progressTrackerFactoryMock.create(any(), any(), any(), anyBoolean()))
            .thenReturn(mock(org.neo4j.gds.core.utils.progress.tasks.ProgressTracker.class));

        var facade = new PathFindingComputeFacade(
            catalogServiceMock,
            algorithmCaller,
            mock(User.class),
            DatabaseId.DEFAULT,
            DefaultPool.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            progressTrackerFactoryMock
        );

        var future = facade.kSpanningTree(
            new GraphName("foo"),
            GRAPH_PARAMETERS,
            Optional.empty(),
            new KSpanningTreeParameters(
                PrimOperators.MIN_OPERATOR,
                idFunction.of("a"),
                2L,
                new Concurrency(2)
            ),
            mock(JobId.class),
            false
        );
        assertThat(future.join()).isNotNull();
    }

}
