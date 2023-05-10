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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.neo4j.gds.core.loading.GraphStoreWithConfig;
import org.neo4j.gds.core.loading.ImmutableCatalogRequest;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.executor.ExecutionContext;
import org.neo4j.gds.executor.ImmutableExecutionContext;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.gds.catalog.GraphSampleProc.CNARW_CONFIG_PROVIDER;
import static org.neo4j.gds.catalog.GraphSampleProc.CNARW_PROVIDER;
import static org.neo4j.gds.catalog.GraphSampleProc.RWR_CONFIG_PROVIDER;
import static org.neo4j.gds.catalog.GraphSampleProc.RWR_PROVIDER;

@GdlExtension
class SamplerOperatorTest  {

    @GdlGraph
    private static final String DB_CYPHER =
        "CREATE" +
            "  (x:Z {prop: 42})" +
            ", (x1:Z {prop: 43})" +
            ", (x2:Z {prop: 44})" +
            ", (x3:Z {prop: 45})" +
            ", (a:N {prop: 46})" +
            ", (b:N {prop: 47})" +
            ", (c:N {prop: 48, attr: 48})" +
            ", (d:N {prop: 49, attr: 48})" +
            ", (e:M {prop: 50, attr: 48})" +
            ", (f:M {prop: 51, attr: 48})" +
            ", (g:M {prop: 52})" +
            ", (h:M {prop: 53})" +
            ", (i:X {prop: 54})" +
            ", (j:M {prop: 55})" +
            ", (x)-[:R1]->(x1)" +
            ", (x)-[:R1]->(x2)" +
            ", (x)-[:R1]->(x3)" +
            ", (e)-[:R1]->(d)" +
            ", (i)-[:R1]->(g)" +
            ", (a)-[:R1 {cost: 10.0, distance: 5.8}]->(b)" +
            ", (a)-[:R1 {cost: 10.0, distance: 4.8}]->(c)" +
            ", (c)-[:R1 {cost: 10.0, distance: 5.8}]->(d)" +
            ", (d)-[:R1 {cost:  4.2, distance: 2.6}]->(e)" +
            ", (e)-[:R1 {cost: 10.0, distance: 5.8}]->(f)" +
            ", (f)-[:R1 {cost: 10.0, distance: 9.9}]->(g)" +
            ", (h)-[:R2 {cost: 10.0, distance: 5.8}]->(i)";

    @Inject
    private GraphStore graphStore;


    @BeforeEach
    void setUp() {
        GraphStoreCatalog.set(GraphProjectFromStoreConfig.emptyWithName("user", "graph"), graphStore);
    }

    @AfterEach
    void tearDown() {
        GraphStoreCatalog.removeAllLoadedGraphs();
    }

    static Stream<Arguments> samplingParameters() {
        return Stream.of(
            arguments(Map.of("samplingRatio", 1.0), 14),
            arguments(Map.of("samplingRatio", 0.5, "concurrency", 1, "randomSeed", 42l), 7)
        );
    }

    @ParameterizedTest
    @MethodSource("samplingParameters")
    void shouldSampleRWR(Map<String, Object> mapConfiguration, long expectedNodeCount) {

        var executionContext = executionContextBuilder()
            .build();

        var result = SamplerOperator.performSampling("graph", "sample",
            mapConfiguration,
            RWR_CONFIG_PROVIDER,
            RWR_PROVIDER,
            executionContext,
            executionContext.username(),
            input -> graphStoreFromCatalog(input, executionContext)
        ).findFirst().get();

        assertThat(result.nodeCount).isEqualTo(expectedNodeCount);

        assertThat(GraphStoreCatalog.exists(
            executionContext.username(),
            executionContext.databaseId().databaseName(),
            "sample"
        )).isTrue();

        var sampledGraphStore = GraphStoreCatalog.get(executionContext.username(),
            executionContext.databaseId().databaseName(), "sample"
        ).graphStore();
        assertThat(sampledGraphStore.nodeCount()).isEqualTo(expectedNodeCount);
    }

    @ParameterizedTest
    @MethodSource("samplingParameters")
    void shouldSampleCNARW(Map<String, Object> mapConfiguration, long expectedNodeCount) {
        var executionContext = executionContextBuilder()
            .build();

        var result = SamplerOperator.performSampling("graph", "sample",
            mapConfiguration,
            CNARW_CONFIG_PROVIDER,
            CNARW_PROVIDER,
            executionContext,
            executionContext.username(),
            input -> graphStoreFromCatalog(input, executionContext)
        ).findFirst().get();

        assertThat(result.nodeCount).isEqualTo(expectedNodeCount);
        assertThat(GraphStoreCatalog.exists(
            executionContext.username(),
            executionContext.databaseId().databaseName(),
            "sample"
        )).isTrue();

        var sampledGraphStore = GraphStoreCatalog.get(executionContext.username(),
            executionContext.databaseId().databaseName(), "sample"
        ).graphStore();
        assertThat(sampledGraphStore.nodeCount()).isEqualTo(expectedNodeCount);
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

    GraphStoreWithConfig graphStoreFromCatalog(String graphName, ExecutionContext executionContext) {
        var catalogRequest = ImmutableCatalogRequest.of(
            executionContext.databaseId().databaseName(),
            executionContext.username(),
            Optional.empty(),
            executionContext.isGdsAdmin()
        );
        return GraphStoreCatalog.get(catalogRequest, graphName);
    }

}
