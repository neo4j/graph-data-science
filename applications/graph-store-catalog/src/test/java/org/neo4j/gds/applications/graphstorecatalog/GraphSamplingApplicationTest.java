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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.api.GraphName;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.User;
import org.neo4j.gds.compat.Neo4jProxy;
import org.neo4j.gds.compat.TestLog;
import org.neo4j.gds.config.GraphProjectConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.loading.GraphStoreCatalogService;
import org.neo4j.gds.core.utils.progress.EmptyTaskRegistryFactory;
import org.neo4j.gds.core.utils.warnings.EmptyUserLogRegistryFactory;
import org.neo4j.gds.extension.GdlExtension;
import org.neo4j.gds.extension.GdlGraph;
import org.neo4j.gds.extension.IdFunction;
import org.neo4j.gds.extension.Inject;
import org.neo4j.gds.logging.Log;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

@GdlExtension
class GraphSamplingApplicationTest {

    @GdlGraph(idOffset = 42)
    private static final String DB_CYPHER = "CREATE" +
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

    @Inject
    private IdFunction idFunction;

    @BeforeEach
    void setUp() {
        GraphStoreCatalog.set(GraphProjectConfig.emptyWithName("user", "graph"), graphStore);
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
        var graphSamplingApplication = new GraphSamplingApplication(
            new Neo4jBackedLogForTesting(),
            new GraphStoreCatalogService()
        );

        var user = new User("user", false);
        var result = graphSamplingApplication.sample(
            user,
            EmptyTaskRegistryFactory.INSTANCE,
            EmptyUserLogRegistryFactory.INSTANCE,
            graphStore,
            GraphProjectConfig.emptyWithName("user", "graph"),
            GraphName.parse("graph"),
            GraphName.parse("sample"),
            mapConfiguration,
            SamplerCompanion.RWR_CONFIG_PROVIDER,
            SamplerCompanion.RWR_PROVIDER
        );

        assertThat(result.nodeCount).isEqualTo(expectedNodeCount);

        assertThat(
            GraphStoreCatalog.exists(
                user.getUsername(),
                graphStore.databaseInfo().databaseId().databaseName(),
                "sample"
            )
        ).isTrue();

        var sampledGraphStore = GraphStoreCatalog.get(
            user.getUsername(),
            graphStore.databaseInfo().databaseId().databaseName(),
            "sample"
        ).graphStore();
        assertThat(sampledGraphStore.nodeCount()).isEqualTo(expectedNodeCount);
    }

    @ParameterizedTest
    @MethodSource("samplingParameters")
    void shouldSampleCNARW(Map<String, Object> mapConfiguration, long expectedNodeCount) {
        var graphSamplingApplication = new GraphSamplingApplication(
            new Neo4jBackedLogForTesting(),
            new GraphStoreCatalogService()
        );

        var user = new User("user", false);
        var result = graphSamplingApplication.sample(
            user,
            EmptyTaskRegistryFactory.INSTANCE,
            EmptyUserLogRegistryFactory.INSTANCE,
            graphStore,
            GraphProjectConfig.emptyWithName("user", "graph"),
            GraphName.parse("graph"),
            GraphName.parse("sample"),
            mapConfiguration,
            SamplerCompanion.CNARW_CONFIG_PROVIDER,
            SamplerCompanion.CNARW_PROVIDER
        );

        assertThat(result.nodeCount).isEqualTo(expectedNodeCount);
        assertThat(
            GraphStoreCatalog.exists(
                user.getUsername(),
                graphStore.databaseInfo().databaseId(),
                "sample"
            )
        ).isTrue();

        var sampledGraphStore = GraphStoreCatalog.get(
            user.getUsername(),
            graphStore.databaseInfo().databaseId(),
            "sample"
        ).graphStore();
        assertThat(sampledGraphStore.nodeCount()).isEqualTo(expectedNodeCount);
    }

    @ParameterizedTest
    @CsvSource(value = {"0.28,1", "0.35,2"})
    void shouldUseSingleStartNodeRWR(double samplingRatio, long expectedStartNodeCount) {
        var graphSamplingApplication = new GraphSamplingApplication(
            new Neo4jBackedLogForTesting(),
            new GraphStoreCatalogService()
        );

        var user = new User("user", false);
        var x = idFunction.of("x");
        var result = graphSamplingApplication.sample(
            user,
            EmptyTaskRegistryFactory.INSTANCE,
            EmptyUserLogRegistryFactory.INSTANCE,
            graphStore,
            GraphProjectConfig.emptyWithName("user", "graph"),
            GraphName.parse("graph"),
            GraphName.parse("sample"),
            Map.of(
                "samplingRatio",
                samplingRatio,
                "concurrency",
                1,
                "startNodes",
                List.of(x),
                "randomSeed",
                42L
            ),
            SamplerCompanion.RWR_CONFIG_PROVIDER,
            SamplerCompanion.RWR_PROVIDER
        );

        assertThat(result.startNodeCount).isEqualTo(expectedStartNodeCount);

        assertThat(
            GraphStoreCatalog.exists(
                user.getUsername(),
                graphStore.databaseInfo().databaseId(),
                "sample"
            )
        ).isTrue();

    }

    @ParameterizedTest
    @CsvSource(value = {"0.28,1", "0.35,2"})
    void shouldUseSingleStartNodeCNARW(double samplingRatio, long expectedStartNodeCount) {
        var graphSamplingApplication = new GraphSamplingApplication(
            new Neo4jBackedLogForTesting(),
            new GraphStoreCatalogService()
        );

        var user = new User("user", false);
        var x = idFunction.of("x");
        var result = graphSamplingApplication.sample(
            user,
            EmptyTaskRegistryFactory.INSTANCE,
            EmptyUserLogRegistryFactory.INSTANCE,
            graphStore,
            GraphProjectConfig.emptyWithName("user", "graph"),
            GraphName.parse("graph"),
            GraphName.parse("sample"),
            Map.of(
                "samplingRatio",
                samplingRatio,
                "concurrency",
                1,
                "startNodes",
                List.of(x),
                "randomSeed",
                42L
            ),
            SamplerCompanion.CNARW_CONFIG_PROVIDER,
            SamplerCompanion.CNARW_PROVIDER
        );

        assertThat(result.startNodeCount).isEqualTo(expectedStartNodeCount);

        assertThat(
            GraphStoreCatalog.exists(
                user.getUsername(),
                graphStore.databaseInfo().databaseId(),
                "sample"
            )
        ).isTrue();

    }

    /**
     * @deprecated We need this just long enough that we can drive out usages of Neo4j's log.
     *             Therefore, I do not want to build general support for this
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
        public Object getNeo4jLog() {
            return neo4jLog;
        }
    }
}
