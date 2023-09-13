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
package org.neo4j.gds.core.loading;

import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.gds.NodeLabel;
import org.neo4j.gds.RelationshipType;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.config.AlgoBaseConfig;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@GdlExtension
@ExtendWith(SoftAssertionsExtension.class)
class GraphStoreCatalogServiceGetGraphTest {

    @GdlGraph
    private static final String TEST_GRAPH =
        "CREATE" +
            "  (a:N)" +
            ", (b:N)" +
            ", (f:N1)" +
            ", (g:N1)" +
            ", (a)-[:T]->(b)" +
            ", (b)-[:T]->(f)" +
            ", (f)-[:T1]->(g)" +
            ", (g)-[:T1]->(f)" +
            ", (g)-[:T1]->(a)";

    @Inject
    GraphStore graphStore;

    @Test
    void shouldWorkWithoutAnyFilters(SoftAssertions assertions) {
        var serviceSpy = spy(GraphStoreCatalogService.class);
        var graphStoreWithConfigMock = mock(GraphStoreWithConfig.class);
        when(graphStoreWithConfigMock.graphStore()).thenReturn(graphStore);
        doReturn(graphStoreWithConfigMock).when(serviceSpy).get(any(), any());

        var configMock = mock(AlgoBaseConfig.class);
        when(configMock.nodeLabelsFilter()).thenReturn(Collections.emptySet());
        when(configMock.projectAllRelationshipTypes()).thenReturn(true);

        var graphWithGraphStore = serviceSpy.getGraphWithGraphStore(
            GraphName.parse("bogus"),
            configMock,
            Optional.empty(),
            new User("bogusUser", false),
            DatabaseId.EMPTY
        );

        assertThat(graphWithGraphStore.getRight()).isSameAs(graphStore);
        assertThat(graphWithGraphStore.getLeft())
            .isInstanceOf(Graph.class)
            .satisfies(graph -> {
                assertions.assertThat(graph.isEmpty()).isFalse();
                assertions.assertThat(graph.nodeCount())
                    .as("Unexpected node count")
                    .isEqualTo(4);
                assertions.assertThat(graph.relationshipCount())
                    .as("Unexpected relationship count")
                    .isEqualTo(5);

                assertions.assertThat(graph.schema().nodeSchema().availableLabels().stream().map(NodeLabel::name))
                    .containsExactlyInAnyOrder("N", "N1");

                assertions.assertThat(graph.schema()
                        .relationshipSchema()
                        .availableTypes()
                        .stream()
                        .map(RelationshipType::name))
                    .containsExactlyInAnyOrder("T", "T1");
            });
    }

    @Test
    void shouldWorkWithNodeLabels(SoftAssertions assertions) {
        var serviceSpy = spy(GraphStoreCatalogService.class);
        var graphStoreWithConfigMock = mock(GraphStoreWithConfig.class);
        when(graphStoreWithConfigMock.graphStore()).thenReturn(graphStore);
        doReturn(graphStoreWithConfigMock).when(serviceSpy).get(any(), any());

        var configMock = mock(AlgoBaseConfig.class);
        when(configMock.nodeLabelsFilter()).thenReturn(Set.of(NodeLabel.of("N")));
        when(configMock.projectAllRelationshipTypes()).thenReturn(true);

        var graphWithGraphStore = serviceSpy.getGraphWithGraphStore(
            GraphName.parse("bogus"),
            configMock,
            Optional.empty(),
            new User("bogusUser", false),
            DatabaseId.EMPTY
        );

        assertThat(graphWithGraphStore.getRight()).isSameAs(graphStore);
        assertThat(graphWithGraphStore.getLeft())
            .isInstanceOf(Graph.class)
            .satisfies(graph -> {
                assertions.assertThat(graph.isEmpty()).isFalse();
                assertions.assertThat(graph.nodeCount())
                    .as("Unexpected node count")
                    .isEqualTo(2);
                assertions.assertThat(graph.relationshipCount())
                    .as("Unexpected relationship count")
                    .isEqualTo(1);

                assertions.assertThat(graph.schema().nodeSchema().availableLabels().stream().map(NodeLabel::name))
                    .containsExactly("N");

                assertions.assertThat(graph.schema()
                        .relationshipSchema()
                        .availableTypes()
                        .stream()
                        .map(RelationshipType::name))
                    .containsExactlyInAnyOrder("T", "T1");
            });
    }

    @Test
    void shouldWorkWithRelationshipTypes(SoftAssertions assertions) {
        var serviceSpy = spy(GraphStoreCatalogService.class);
        var graphStoreWithConfigMock = mock(GraphStoreWithConfig.class);
        when(graphStoreWithConfigMock.graphStore()).thenReturn(graphStore);
        doReturn(graphStoreWithConfigMock).when(serviceSpy).get(any(), any());

        var configMock = mock(AlgoBaseConfig.class);
        when(configMock.nodeLabelsFilter()).thenReturn(Collections.emptySet());
        when(configMock.relationshipTypesFilter()).thenReturn(Set.of(RelationshipType.of("T")));

        var graphWithGraphStore = serviceSpy.getGraphWithGraphStore(
            GraphName.parse("bogus"),
            configMock,
            Optional.empty(),
            new User("bogusUser", false),
            DatabaseId.EMPTY
        );

        assertThat(graphWithGraphStore.getRight()).isSameAs(graphStore);
        assertThat(graphWithGraphStore.getLeft())
            .isInstanceOf(Graph.class)
            .satisfies(graph -> {

                assertions.assertThat(graph.isEmpty()).isFalse();
                assertions.assertThat(graph.nodeCount())
                    .as("Unexpected node count")
                    .isEqualTo(4);
                assertions.assertThat(graph.relationshipCount())
                    .as("Unexpected relationship count")
                    .isEqualTo(2);

                assertions.assertThat(graph.schema().nodeSchema().availableLabels().stream().map(NodeLabel::name))
                    .containsExactlyInAnyOrder("N", "N1");

                assertions.assertThat(graph.schema()
                        .relationshipSchema()
                        .availableTypes()
                        .stream()
                        .map(RelationshipType::name))
                    .containsExactly("T");
            });
    }

    @Test
    void shouldWorkWithNodeLabelsAndRelationshipTypes(SoftAssertions assertions) {
        var serviceSpy = spy(GraphStoreCatalogService.class);
        var graphStoreWithConfigMock = mock(GraphStoreWithConfig.class);
        when(graphStoreWithConfigMock.graphStore()).thenReturn(graphStore);
        doReturn(graphStoreWithConfigMock).when(serviceSpy).get(any(), any());

        var configMock = mock(AlgoBaseConfig.class);
        when(configMock.nodeLabelsFilter()).thenReturn(Set.of(NodeLabel.of("N")));
        when(configMock.relationshipTypesFilter()).thenReturn(Set.of(RelationshipType.of("T")));

        var graphWithGraphStore = serviceSpy.getGraphWithGraphStore(
            GraphName.parse("bogus"),
            configMock,
            Optional.empty(),
            new User("bogusUser", false),
            DatabaseId.EMPTY
        );

        assertThat(graphWithGraphStore.getRight()).isSameAs(graphStore);
        assertThat(graphWithGraphStore.getLeft())
            .isInstanceOf(Graph.class)
            .satisfies(graph -> {

                assertions.assertThat(graph.isEmpty()).isFalse();
                assertions.assertThat(graph.nodeCount())
                    .as("Unexpected node count")
                    .isEqualTo(2);
                assertions.assertThat(graph.relationshipCount())
                    .as("Unexpected relationship count")
                    .isEqualTo(1);

                assertions.assertThat(graph.schema().nodeSchema().availableLabels().stream().map(NodeLabel::name))
                    .containsExactly("N");

                assertions.assertThat(graph.schema()
                        .relationshipSchema()
                        .availableTypes()
                        .stream()
                        .map(RelationshipType::name))
                    .containsExactly("T");
            });
    }

    @Test
    void shouldReturnGraphWithNoRelationshipsForEmptyRelationshipTypeFilter(SoftAssertions assertions) {
        var serviceSpy = spy(GraphStoreCatalogService.class);
        var graphStoreWithConfigMock = mock(GraphStoreWithConfig.class);
        when(graphStoreWithConfigMock.graphStore()).thenReturn(graphStore);
        doReturn(graphStoreWithConfigMock).when(serviceSpy).get(any(), any());

        var configMock = mock(AlgoBaseConfig.class);
        when(configMock.nodeLabelsFilter()).thenReturn(Set.of(NodeLabel.of("N")));
        when(configMock.projectAllRelationshipTypes()).thenReturn(false);
        when(configMock.relationshipTypes()).thenReturn(Collections.emptyList());

        var graphWithGraphStore = serviceSpy.getGraphWithGraphStore(
            GraphName.parse("bogus"),
            configMock,
            Optional.empty(),
            new User("bogusUser", false),
            DatabaseId.EMPTY
        );

        assertThat(graphWithGraphStore.getRight()).isSameAs(graphStore);
        assertThat(graphWithGraphStore.getLeft())
            .isInstanceOf(Graph.class)
            .satisfies(graph -> {

                assertions.assertThat(graph.isEmpty()).isFalse();
                assertions.assertThat(graph.nodeCount())
                    .as("Unexpected node count")
                    .isEqualTo(2);
                assertions.assertThat(graph.relationshipCount())
                    .as("Unexpected relationship count")
                    .isZero();

                assertions.assertThat(graph.schema().nodeSchema().availableLabels().stream().map(NodeLabel::name))
                    .containsExactly("N");

                assertions.assertThat(graph.schema().relationshipSchema().availableTypes())
                    .as("The relationship schema should not contain any types")
                    .isEmpty();
            });
    }
}
