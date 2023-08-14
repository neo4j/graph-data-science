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
package org.neo4j.gds.algorithms.community;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.algorithms.AlgorithmMemoryValidationService;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.extension.TestGraph;
import org.neo4j.gds.wcc.WccBaseConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CommunityAlgorithmsBusinessFacadeTest {

    @Nested
    @GdlExtension
    class WccTest {
        @GdlGraph
        private static final String TEST_GRAPH =
            "CREATE" +
                "  (a:Node)" +
                ", (b:Node)" +
                ", (c:Node)" +
                ", (d:Node)" +
                ", (e:Node)" +
                ", (f:Node)" +
                ", (g:Node)" +
                ", (h:Node)" +
                ", (i:Node)" +
                // {J}
                ", (j:Node)" +
                // {A, B, C, D}
                ", (a)-[:TYPE]->(b)" +
                ", (b)-[:TYPE]->(c)" +
                ", (c)-[:TYPE]->(d)" +
                ", (d)-[:TYPE]->(a)" +
                // {E, F, G}
                ", (e)-[:TYPE]->(f)" +
                ", (f)-[:TYPE]->(g)" +
                ", (g)-[:TYPE]->(e)" +
                // {H, I}
                ", (i)-[:TYPE]->(h)" +
                ", (h)-[:TYPE]->(i)";

        @Inject
        TestGraph graph;

        @Inject
        GraphStore graphStore;

        @Test
        void wcc() {
            // given
            var graphStoreCatalogServiceMock = mock(GraphStoreCatalogService.class);
            doReturn(Pair.of(graph, graphStore))
                .when(graphStoreCatalogServiceMock)
                .getGraphWithGraphStore(any(), any(), any(), any(), any());

            var config = mock(WccBaseConfig.class);
            var algorithmsBusinessFacade = new CommunityAlgorithmsBusinessFacade(
                new CommunityAlgorithmsFacade(
                    graphStoreCatalogServiceMock,
                    mock(AlgorithmMemoryValidationService.class)
                ), null
            );

            // when
            var wccComputationResult = algorithmsBusinessFacade.streamWcc(
                "meh",
                config,
                null,
                null,
                ProgressTracker.NULL_TRACKER
            );

            //then
            assertThat(wccComputationResult.result())
                .isNotEmpty()
                .get()
                .satisfies(disjointSetStruct -> {
                    assertThat(disjointSetStruct.size()).isEqualTo(10);
                });
            assertThat(wccComputationResult.graph()).isSameAs(graph);
            assertThat(wccComputationResult.graphStore()).isSameAs(graphStore);
            assertThat(wccComputationResult.configuration()).isSameAs(config);
        }

        @Test
        void wccOnEmptyGraph() {
            // given
            var graphStoreCatalogServiceMock = mock(GraphStoreCatalogService.class);
            var graphMock = mock(Graph.class);
            when(graphMock.isEmpty()).thenReturn(true);
            doReturn(Pair.of(graphMock, mock(GraphStore.class)))
                .when(graphStoreCatalogServiceMock)
                .getGraphWithGraphStore(any(), any(), any(), any(), any());
            var algorithmsBusinessFacade = new CommunityAlgorithmsBusinessFacade(
                new CommunityAlgorithmsFacade(graphStoreCatalogServiceMock, null), null
            );

            // when
            var wccComputationResult = algorithmsBusinessFacade.streamWcc("meh", mock(WccBaseConfig.class), null, null, null);

            //then
            assertThat(wccComputationResult.result()).isEmpty();
        }
    }
}
