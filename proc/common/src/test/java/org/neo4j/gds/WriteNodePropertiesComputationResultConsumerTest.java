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
package org.neo4j.gds;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.api.AlgorithmMetaDataSetter;
import org.neo4j.gds.api.CloseableResourceRegistry;
import org.neo4j.gds.api.DatabaseId;
import org.neo4j.gds.api.EmptyDependencyResolver;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.GraphCharacteristics;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.GraphStoreAdapter;
import org.neo4j.gds.api.NodeLookup;
import org.neo4j.gds.api.ProcedureReturnColumns;
import org.neo4j.gds.api.PropertyState;
import org.neo4j.gds.api.TerminationMonitor;
import org.neo4j.gds.api.Topology;
import org.neo4j.gds.api.properties.nodes.EmptyLongNodePropertyValues;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.api.schema.GraphSchema;
import org.neo4j.gds.api.schema.ImmutableMutableGraphSchema;
import org.neo4j.gds.api.schema.MutableNodeSchema;
import org.neo4j.gds.api.schema.MutableRelationshipSchema;
import org.neo4j.gds.api.schema.PropertySchema;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.config.ImmutableArrowConnectionInfo;
import org.neo4j.gds.core.huge.HugeGraphBuilder;
import org.neo4j.gds.core.loading.Capabilities;
import org.neo4j.gds.core.loading.Capabilities.WriteMode;
import org.neo4j.gds.core.loading.ImmutableStaticCapabilities;
import org.neo4j.gds.core.model.ModelCatalog;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.core.write.ImmutableNodeProperty;
import org.neo4j.gds.core.write.NativeNodePropertiesExporterBuilder;
import org.neo4j.gds.executor.ComputationResult;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ImmutableComputationResult;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.test.ImmutableTestWriteConfig;
import org.neo4j.gds.test.TestAlgoResultBuilder;
import org.neo4j.gds.test.TestAlgorithm;
import org.neo4j.gds.test.TestAlgorithmResult;
import org.neo4j.gds.test.TestResult;
import org.neo4j.gds.test.TestWriteConfig;
import org.neo4j.gds.transaction.EmptyTransactionContext;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@GdlExtension
class WriteNodePropertiesComputationResultConsumerTest extends BaseTest {

    @GdlGraph(graphNamePrefix = "base")
    static final String DB_CYPHER = "(a)";

    @Inject
    private GraphStore baseGraphStore;

    private final ExecutionContext executionContext = ImmutableExecutionContext
        .builder()
        .databaseId(DatabaseId.from(""))
        .dependencyResolver(EmptyDependencyResolver.INSTANCE)
        .returnColumns(ProcedureReturnColumns.EMPTY)
        .log(Neo4jProxy.testLog())
        .taskRegistryFactory(EmptyTaskRegistryFactory.INSTANCE)
        .userLogRegistryFactory(EmptyUserLogRegistryFactory.INSTANCE)
        .username("")
        .terminationMonitor(TerminationMonitor.EMPTY)
        .closeableResourceRegistry(CloseableResourceRegistry.EMPTY)
        .algorithmMetaDataSetter(AlgorithmMetaDataSetter.EMPTY)
        .nodeLookup(NodeLookup.EMPTY)
        .modelCatalog(ModelCatalog.EMPTY)
        .isGdsAdmin(false)
        .nodePropertyExporterBuilder(new NativeNodePropertiesExporterBuilder(EmptyTransactionContext.INSTANCE))
        .build();

    @Test
    void shouldThrowWhenWriteModeDoesNotMatchPropertyState() {
        var config = ImmutableTestWriteConfig.builder().writeProperty("writeProp").build();

        assertThatThrownBy(() -> executeWrite(config, PropertyState.TRANSIENT, WriteMode.LOCAL))
            .hasMessageContaining("propertyKey=foo, propertyState=TRANSIENT");

        assertThatThrownBy(() -> executeWrite(config, PropertyState.REMOTE, WriteMode.LOCAL))
            .hasMessageContaining("propertyKey=foo, propertyState=REMOTE");
    }

    @Test
    void shouldThrowWhenArrowConnectionInfoIsMissing() {
        var config = ImmutableTestWriteConfig.builder().writeProperty("writeProp").build();

        assertThatThrownBy(() -> executeWrite(config, PropertyState.REMOTE, WriteMode.REMOTE))
            .hasMessageContaining("Missing arrow connection info");
    }

    @Test
    void shouldThrowWhenArrowConnectionInfoIsGivenForLocalWriteBack() {
        var config = ImmutableTestWriteConfig.builder()
            .writeProperty("writeProp")
            .arrowConnectionInfo(ImmutableArrowConnectionInfo.of("localhost", 1337, "token"))
            .build();

        assertThatThrownBy(() -> executeWrite(config, PropertyState.PERSISTENT, WriteMode.LOCAL))
            .hasMessageContaining("write operation is targeting a local database");
    }

    @Test
    void shouldThrowWhenWriteModeIsNone() {
        var config = ImmutableTestWriteConfig.builder().writeProperty("writeProp").build();

        assertThatThrownBy(() -> executeWrite(config, PropertyState.PERSISTENT, WriteMode.NONE))
            .hasMessageContaining("cannot write back to a database");
    }

    private void executeWrite(
        TestWriteConfig writeConfig, PropertyState propertyState,
        WriteMode writeMode
    ) {
        var propertyKey = "foo";
        var graphStore = new GraphStoreWithNodeProperty(
            baseGraphStore,
            propertyKey,
            EmptyLongNodePropertyValues.INSTANCE,
            propertyState,
            writeMode
        );
        var writeConsumer = new WriteNodePropertiesComputationResultConsumer<TestAlgorithm, TestAlgorithmResult, TestWriteConfig, TestResult>(
            (computationResult, executionContext) -> new TestAlgoResultBuilder(),
            computationResult -> List.of(ImmutableNodeProperty.of(
                propertyKey,
                graphStore.nodeProperty(propertyKey).values()
            )),
            "TestAlgorithm"
        );

        var computationResult = getComputationResult(graphStore, graphStore.getUnion(), writeConfig);
        writeConsumer.consume(computationResult, executionContext);
    }

    private static ComputationResult<TestAlgorithm, TestAlgorithmResult, TestWriteConfig> getComputationResult(
        GraphStore graphStore,
        Graph graph,
        TestWriteConfig config
    ) {
        var algorithm = new TestAlgorithm(graph, ProgressTracker.NULL_TRACKER, false);

        return ImmutableComputationResult.<TestAlgorithm, TestAlgorithmResult, TestWriteConfig>builder()
            .algorithm(algorithm)
            .config(config)
            .result(algorithm.compute())
            .graph(graph)
            .graphStore(graphStore)
            .preProcessingMillis(0)
            .computeMillis(0)
            .build();
    }

    private static class GraphStoreWithNodeProperty extends GraphStoreAdapter {

        private final String propertyKey;
        private final NodePropertyValues nodePropertyValues;
        private final WriteMode writeMode;
        private final PropertySchema propertySchema;

        GraphStoreWithNodeProperty(
            GraphStore graphStore,
            String propertyKey,
            NodePropertyValues nodePropertyValues,
            PropertyState propertyState,
            WriteMode writeMode
        ) {
            super(graphStore);
            this.propertyKey = propertyKey;
            this.nodePropertyValues = nodePropertyValues;
            this.writeMode = writeMode;
            this.propertySchema = PropertySchema.of(
                propertyKey,
                nodePropertyValues.valueType(),
                nodePropertyValues.valueType().fallbackValue(),
                propertyState
            );
        }

        @Override
        public Capabilities capabilities() {
            return ImmutableStaticCapabilities.of(writeMode);
        }

        @Override
        public org.neo4j.gds.api.properties.nodes.NodeProperty nodeProperty(String propertyKey) {
            if (this.propertyKey.equals(propertyKey)) {
                return org.neo4j.gds.api.properties.nodes.ImmutableNodeProperty.of(nodePropertyValues, propertySchema);
            }
            throw new RuntimeException();
        }

        @Override
        public GraphSchema schema() {
            return ImmutableMutableGraphSchema.of(
                Map.of(),
                MutableNodeSchema.empty().addProperty(NodeLabel.ALL_NODES, propertyKey, propertySchema),
                MutableRelationshipSchema.empty()
            );
        }

        @Override
        public Graph getUnion() {
            return new HugeGraphBuilder()
                .characteristics(GraphCharacteristics.NONE)
                .topology(Topology.EMPTY)
                .nodes(nodes())
                .schema(schema())
                .nodeProperties(Map.of(propertyKey, nodePropertyValues))
                .build();
        }
    }
}