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
package org.neo4j.gds.applications.graphstorecatalog;

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
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.ResultStore;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.write.NodeProperty;
import org.neo4j.gds.core.write.NodePropertyExporter;
import org.neo4j.gds.core.write.NodePropertyExporterBuilder;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.Log;
import org.neo4j.gds.termination.TerminationFlag;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@GdlExtension
@ExtendWith(MockitoExtension.class)
class WriteNodePropertiesApplicationTest {
    @Captor
    ArgumentCaptor<List<NodeProperty>> captor;

    @SuppressWarnings("unused")
    @GdlGraph
    private static final String GDL =
        "CREATE" +
            "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
            ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
            ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
            ", (d:B {nodeProp1: 3, nodeProp2: 45})" +
            ", (e:B {nodeProp1: 4, nodeProp2: 46})" +
            ", (f:B {nodeProp1: 5, nodeProp2: 47})";

    @SuppressWarnings("unused")
    @Inject
    private GraphStore graphStore;

    @SuppressWarnings("unused")
    @GdlGraph(graphNamePrefix = "propertiesSubset")
    private static final String GDL_PROPERTIES_SUBSET =
        "CREATE" +
            "  (a:A {nodeProp1: 0, nodeProp2: 42})" +
            ", (b:A {nodeProp1: 1, nodeProp2: 43})" +
            ", (c:A {nodeProp1: 2, nodeProp2: 44})" +
            ", (d:B {nodeProp1: 3})" +
            ", (e:B {nodeProp1: 4})" +
            ", (f:B {nodeProp1: 5})";

    @SuppressWarnings("unused")
    @Inject
    private GraphStore propertiesSubsetGraphStore;

    @BeforeEach
    void setUp() {
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("user", "g"), graphStore);
        GraphStoreCatalog.set(
            GraphProjectConfig.emptyWithName("user", "propertiesSubsetGraph"),
            propertiesSubsetGraphStore
        );
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("nodeLabels")
    void writeNodeProperties(Object nodeLabels, String displayName) {
        var nodePropertiesWriter = new WriteNodePropertiesApplication(new Neo4jBackedLogForTesting());

        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);
        when(nodePropertyExporterMock.propertiesWritten()).thenReturn(8L, 11L);
        var writeResult = nodePropertiesWriter.write(
            graphStore,
            ResultStore.EMPTY,
            exporterBuilderMock,
            EmptyTaskRegistryFactory.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            EmptyUserLogRegistryFactory.INSTANCE,
            GraphName.parse("g"),
            GraphWriteNodePropertiesConfig.of(
                "g",
                List.of("nodeProp1", "nodeProp2"),
                nodeLabels,
                CypherMapWrapper.empty()
            )
        );

        assertThat(writeResult.writeMillis).isGreaterThan(-1);
        assertThat(writeResult.graphName).isEqualTo("g");
        assertThat(writeResult.nodeProperties).containsExactly("nodeProp1", "nodeProp2");
        assertThat(writeResult.propertiesWritten).isEqualTo(8L);
        assertThat(writeResult.configuration).isNotNull();

        verify(nodePropertyExporterMock, times(1)).propertiesWritten();
        verify(nodePropertyExporterMock, times(1)).write(captor.capture());
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
        var nodePropertiesWriter = new WriteNodePropertiesApplication(new Neo4jBackedLogForTesting());

        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);
        when(nodePropertyExporterMock.propertiesWritten()).thenReturn(11L);
        var writeResult = nodePropertiesWriter.write(
            propertiesSubsetGraphStore,
            ResultStore.EMPTY,
            exporterBuilderMock,
            EmptyTaskRegistryFactory.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            EmptyUserLogRegistryFactory.INSTANCE,
            GraphName.parse("propertiesSubsetGraph"),
            GraphWriteNodePropertiesConfig.of(
                "propertiesSubsetGraph",
                List.of("nodeProp1", "nodeProp2"),
                List.of("*"),
                CypherMapWrapper.empty()
            )
        );

        assertThat(writeResult.writeMillis).isGreaterThan(-1);
        assertThat(writeResult.graphName).isEqualTo("propertiesSubsetGraph");
        assertThat(writeResult.nodeProperties).containsExactly("nodeProp1", "nodeProp2");
        assertThat(writeResult.propertiesWritten).isEqualTo(11L);
        assertThat(writeResult.configuration).isNotNull();

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
        var nodePropertiesWriter = new WriteNodePropertiesApplication(new Neo4jBackedLogForTesting());

        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);
        var writeResult = nodePropertiesWriter.write(
            graphStore,
            ResultStore.EMPTY,
            exporterBuilderMock,
            EmptyTaskRegistryFactory.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            EmptyUserLogRegistryFactory.INSTANCE,
            GraphName.parse("g"),
            GraphWriteNodePropertiesConfig.of(
                "g",
                List.of("nodeProp1", "nodeProp2"),
                List.of("A"),
                CypherMapWrapper.empty()
            )
        );

        assertThat(writeResult.writeMillis).isGreaterThan(-1);
        assertThat(writeResult.graphName).isEqualTo("g");
        assertThat(writeResult.nodeProperties).containsExactly("nodeProp1", "nodeProp2");
        assertThat(writeResult.configuration).isNotNull();

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
        var nodePropertiesWriter = new WriteNodePropertiesApplication(new Neo4jBackedLogForTesting());

        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);
        var writeResult = nodePropertiesWriter.write(
            graphStore,
            ResultStore.EMPTY,
            exporterBuilderMock,
            EmptyTaskRegistryFactory.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            EmptyUserLogRegistryFactory.INSTANCE,
            GraphName.parse("g"),
            GraphWriteNodePropertiesConfig.of(
                "g",
                Map.of("nodeProp1", "foo"),
                List.of("*"),
                CypherMapWrapper.empty()
            )
        );
        assertThat(writeResult.writeMillis).isGreaterThan(-1);
        assertThat(writeResult.graphName).isEqualTo("g");
        assertThat(writeResult.nodeProperties).containsExactly("foo");
        assertThat(writeResult.configuration).isNotNull();

        verify(nodePropertyExporterMock, times(1)).propertiesWritten();
        verify(nodePropertyExporterMock, times(1)).write(captor.capture());
        verifyNoMoreInteractions(nodePropertyExporterMock);
        assertThat(captor.getValue())
            .hasSize(1)
            .satisfiesExactly(nodeProperty -> assertThat(nodeProperty.propertyKey()).isEqualTo("foo"));
    }

    @Test
    void shouldRenameMultipleProperties() {
        var nodePropertiesWriter = new WriteNodePropertiesApplication(new Neo4jBackedLogForTesting());

        var exporterBuilderMock = mock(NodePropertyExporterBuilder.class, Answers.RETURNS_SELF);
        var nodePropertyExporterMock = mock(NodePropertyExporter.class);
        when(exporterBuilderMock.build()).thenReturn(nodePropertyExporterMock);
        var writeResult = nodePropertiesWriter.write(
            graphStore,
            ResultStore.EMPTY,
            exporterBuilderMock,
            EmptyTaskRegistryFactory.INSTANCE,
            TerminationFlag.RUNNING_TRUE,
            EmptyUserLogRegistryFactory.INSTANCE,
            GraphName.parse("g"),
            GraphWriteNodePropertiesConfig.of(
                "g",
                List.of(Map.of("nodeProp1", "foo", "nodeProp2", "bar"), "nodeProp1"),
                List.of("A"),
                CypherMapWrapper.empty()
            )
        );

        assertThat(writeResult.writeMillis).isGreaterThan(-1);
        assertThat(writeResult.graphName).isEqualTo("g");
        assertThat(writeResult.nodeProperties).containsExactlyInAnyOrder("foo", "bar", "nodeProp1");
        assertThat(writeResult.configuration).isNotNull();

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

    /**
     * @deprecated We need this just long enough that we can drive out usages of Neo4j's log.
     *     Therefore, I do not want to build general support for this
     */
    @Deprecated
    private static class Neo4jBackedLogForTesting implements Log {
        private final TestLog neo4jLog = Neo4jProxy.testLog();

        @Override
        public void info(String message) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void info(String format, Object... arguments) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void warn(String message, Exception e) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void warn(String format, Object... arguments) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public boolean isDebugEnabled() {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void debug(String format, Object... arguments) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void error(String format, Throwable throwable) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public void error(String messageFormat, Throwable exception, Object... arguments) {
            throw new UnsupportedOperationException("TODO");
        }

        @Override
        public Object getNeo4jLog() {
            return neo4jLog;
        }
    }
}
