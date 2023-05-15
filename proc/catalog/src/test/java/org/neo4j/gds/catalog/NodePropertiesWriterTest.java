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
package org.neo4j.gds.catalog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.EmptyDependencyResolver;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@GdlExtension
@ExtendWith(MockitoExtension.class)
class NodePropertiesWriterTest {

    @Captor
    ArgumentCaptor<List<NodeProperty>> captor;

    @GdlGraph
    private static final String GDL =
        "CREATE" +
            "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
            ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
            ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
            ", (d:B {nodeProp1: 3, nodeProp2: 45})" +
            ", (e:B {nodeProp1: 4, nodeProp2: 46})" +
            ", (f:B {nodeProp1: 5, nodeProp2: 47})";

    @Inject
    private GraphStore graphStore;

    @GdlGraph(graphNamePrefix = "propertiesSubset")
    private static final String GDL_PROPERTIES_SUBSET =
        "CREATE" +
            "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
            ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
            ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
            ", (d:B {nodeProp1: 3})" +
            ", (e:B {nodeProp1: 4})" +
            ", (f:B {nodeProp1: 5})";

    @Inject
    private GraphStore propertiesSubsetGraphStore;

    @BeforeEach
    void setUp() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "g"), graphStore);
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "propertiesSubsetGraph"), propertiesSubsetGraphStore);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("nodeLabels")
    void writeNodeProperties(Object nodeLabels, String displayName) {

        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);
        when(nodePropertyExporterMock.propertiesWritten()).thenReturn(8L, 11L);

        var executionContext = executionContextBuilder()
            .nodePropertyExporterBuilder(exporterBuilderMock)
            .build();

        var writeResult = NodePropertiesWriter.write(
            "g",
            List.of("nodeProp1", "nodeProp2"),
            nodeLabels,
            Map.of(),
            executionContext,
            Optional.empty()
        );

        assertThat(writeResult).hasSize(1).satisfiesExactly(
            nodePropertiesWriteResult -> {
                assertThat(nodePropertiesWriteResult.writeMillis).isGreaterThan(-1);
                assertThat(nodePropertiesWriteResult.graphName).isEqualTo("g");
                assertThat(nodePropertiesWriteResult.nodeProperties).containsExactly("nodeProp1", "nodeProp2");
                assertThat(nodePropertiesWriteResult.propertiesWritten).isEqualTo(19L);
            }
        );

        verify(nodePropertyExporterMock, times(2)).propertiesWritten();
        verify(nodePropertyExporterMock, times(2)).write(captor.capture());
        verifyNoMoreInteractions(nodePropertyExporterMock);
        assertThat(captor.getValue()).hasSize(2).satisfiesExactlyInAnyOrder(
            nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("nodeProp1"),
            nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("nodeProp2")
        );
    }

    public static Stream<Arguments> nodeLabels() {
        return Stream.of(
            Arguments.of("*", "Implicit `all labels`"),
            Arguments.of(List.of("A", "B"), "Explicit `all labels`")
        );
    }

    @Test
    void writeNodePropertiesForLabel() {

        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);
        when(nodePropertyExporterMock.propertiesWritten()).thenReturn(11L);

        var executionContext = executionContextBuilder()
            .nodePropertyExporterBuilder(exporterBuilderMock)
            .build();

        var writeResult = NodePropertiesWriter.write(
            "propertiesSubsetGraph",
            List.of("nodeProp1", "nodeProp2"),
            "*",
            Map.of(),
            executionContext,
            Optional.empty()
        );

        assertThat(writeResult).hasSize(1).satisfiesExactly(
            nodePropertiesWriteResult -> {
                assertThat(nodePropertiesWriteResult.writeMillis).isGreaterThan(-1);
                assertThat(nodePropertiesWriteResult.graphName).isEqualTo("propertiesSubsetGraph");
                assertThat(nodePropertiesWriteResult.nodeProperties).containsExactly("nodeProp1", "nodeProp2");
                assertThat(nodePropertiesWriteResult.propertiesWritten).isEqualTo(11L);
            }
        );

        // Only for NodeLabel `A` because it is the one that has all the properties.
        verify(nodePropertyExporterMock, times(1)).propertiesWritten();
        verify(nodePropertyExporterMock, times(1)).write(captor.capture());
        verifyNoMoreInteractions(nodePropertyExporterMock);
        assertThat(captor.getValue()).hasSize(2).satisfiesExactlyInAnyOrder(
            nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("nodeProp1"),
            nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("nodeProp2")
        );
    }

    @Test
    void writeNodePropertiesForLabelSubset() {
        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);

        var executionContext = executionContextBuilder()
            .nodePropertyExporterBuilder(exporterBuilderMock)
            .build();

        var writeResult = NodePropertiesWriter.write(
            "g",
            List.of("nodeProp1", "nodeProp2"),
            "A",
            Map.of(),
            executionContext,
            Optional.empty()
        );

        assertThat(writeResult).hasSize(1).satisfiesExactly(
            nodePropertiesWriteResult -> {
                assertThat(nodePropertiesWriteResult.writeMillis).isGreaterThan(-1);
                assertThat(nodePropertiesWriteResult.graphName).isEqualTo("g");
                assertThat(nodePropertiesWriteResult.nodeProperties).containsExactly("nodeProp1", "nodeProp2");
            }
        );

        // Only for NodeLabel `A` because it is the one that was requested.
        verify(nodePropertyExporterMock, times(1)).propertiesWritten();
        verify(nodePropertyExporterMock, times(1)).write(captor.capture());
        verifyNoMoreInteractions(nodePropertyExporterMock);
        assertThat(captor.getValue()).hasSize(2).satisfiesExactlyInAnyOrder(
            nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("nodeProp1"),
            nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("nodeProp2")
        );
    }

    @Test
    void shouldRenameSingleProperly() {
        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);

        var executionContext = executionContextBuilder()
            .nodePropertyExporterBuilder(exporterBuilderMock)
            .build();

        var writeResult = NodePropertiesWriter.write(
            "g",
            Map.of("nodeProp1", "foo"),
            "*",
            Map.of(),
            executionContext,
            Optional.empty()
        );

        assertThat(writeResult).hasSize(1).satisfiesExactly(
            nodePropertiesWriteResult -> {
                assertThat(nodePropertiesWriteResult.writeMillis).isGreaterThan(-1);
                assertThat(nodePropertiesWriteResult.graphName).isEqualTo("g");
                assertThat(nodePropertiesWriteResult.nodeProperties).containsExactly("foo");
            }
        );

        verify(nodePropertyExporterMock, times(2)).propertiesWritten();
        verify(nodePropertyExporterMock, times(2)).write(captor.capture());
        verifyNoMoreInteractions(nodePropertyExporterMock);
        assertThat(captor.getValue())
            .hasSize(1)
            .satisfiesExactly(nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("foo"));
    }

    @Test
    void shouldRenameMultipleProperties() {
        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);

        var executionContext = executionContextBuilder()
            .nodePropertyExporterBuilder(exporterBuilderMock)
            .build();

        var writeResult = NodePropertiesWriter.write(
            "g",
            List.of(Map.of("nodeProp1", "foo", "nodeProp2", "bar"), "nodeProp1"),
            "A",
            Map.of(),
            executionContext,
            Optional.empty()
        );

        assertThat(writeResult).hasSize(1).satisfiesExactly(
            nodePropertiesWriteResult -> {
                assertThat(nodePropertiesWriteResult.writeMillis).isGreaterThan(-1);
                assertThat(nodePropertiesWriteResult.graphName).isEqualTo("g");
                assertThat(nodePropertiesWriteResult.nodeProperties).containsExactlyInAnyOrder("foo", "bar", "nodeProp1");
            }
        );

        // Only for NodeLabel `A` because it is the one that was requested.
        verify(nodePropertyExporterMock, times(1)).propertiesWritten();
        verify(nodePropertyExporterMock, times(1)).write(captor.capture());
        verifyNoMoreInteractions(nodePropertyExporterMock);
        assertThat(captor.getValue())
            .hasSize(3)
            .satisfiesExactlyInAnyOrder(
                nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("foo"),
                nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("bar"),
                nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("nodeProp1")
            );

    }

    private ImmutableExecutionContext.Builder executionContextBuilder() {
        return ImmutableExecutionContext
            .builder()
            .databaseId(graphStore.databaseId())
            .dependencyResolver(EmptyDependencyResolver.INSTANCE)
            .returnColumns(ProcedureReturnColumns.EMPTY)
            .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
            .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
            .username("user")
            .terminationMonitor(TerminationMonitor.EMPTY)
            .closeableResourceRegistry(CloseableResourceRegistry.EMPTY)
            .algorithmMetaDataSetter(AlgorithmMetaDataSetter.EMPTY)
            .nodeLookup(NodeLookup.EMPTY)
            .log(Neo4jProxy.testLog())
            .isGdsAdmin(false);
    }

}
