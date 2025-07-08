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

import org.junit.jupiter.api.Test;
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
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@GdlExtension
class PathFindingComputeFacadeTest {

    @GdlGraph
    private static final String GDL =
        """
        (a:Node)-[:REL]->(b:Node),
        (b)-[:REL]->(c:Node),
        (a)-[:REL]->(c)
        """;

    @Inject
    private TestGraph graph;

    @Inject
    private IdFunction idFunction;

    @Test
    void allShortestPaths_onSimpleGraph_returnsExpectedPaths() {

        var catalogServiceMock = mock(GraphStoreCatalogService.class);

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

        var graphParameters = new GraphParameters(
            List.of(NodeLabel.of("Node")),
            List.of(RelationshipType.of("REL")),
            true,
            Optional.empty()
        );

        var future = facade.allShortestPaths(
            new GraphName("foo"),
            graphParameters,
            Optional.empty(),
            new AllShortestPathsParameters(new Concurrency(4), false),
            mock(JobId.class)
        );

        var results = future.join().collect(Collectors.toList());
        long a = idFunction.of("a");
        long c = idFunction.of("c");

        assertThat(results)
            .isNotEmpty()
            .anySatisfy(r -> assertThat(r.sourceNodeId()).isEqualTo(a))
            .anySatisfy(r -> assertThat(r.targetNodeId()).isEqualTo(c));
    }

}
