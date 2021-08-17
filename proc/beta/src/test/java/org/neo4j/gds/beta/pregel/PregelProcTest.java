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
package org.neo4j.gds.beta.pregel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.gds.Algorithm;
import org.neo4j.gds.AlgorithmFactory;
import org.neo4j.gds.BaseProcTest;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.TestProgressEventTracker;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.api.NodeProperties;
import org.neo4j.gds.api.nodeproperties.ValueType;
import org.neo4j.gds.assertj.ConditionFactory;
import org.neo4j.gds.beta.pregel.context.ComputeContext;
import org.neo4j.gds.catalog.GraphCreateProc;
import org.neo4j.gds.config.GraphCreateConfig;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.core.concurrency.Pools;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.core.utils.BatchingProgressLogger;
import org.neo4j.gds.core.utils.mem.AllocationTracker;
import org.neo4j.gds.core.utils.mem.MemoryEstimation;
import org.neo4j.gds.core.utils.mem.MemoryEstimations;
import org.neo4j.gds.core.utils.progress.tasks.ProgressTracker;
import org.neo4j.gds.core.utils.progress.tasks.Tasks;
import org.neo4j.gds.result.AbstractResultBuilder;
import org.neo4j.gds.test.TestPregelConfig;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.gds.TestSupport.assertGraphEquals;
import static org.neo4j.gds.TestSupport.fromGdl;
import static org.neo4j.gds.compat.GraphDatabaseApiProxy.newKernelTransaction;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class PregelProcTest extends BaseProcTest {

    @BeforeEach
    void setup() throws Exception {
        runQuery("CREATE (:OffsetId), (:RealNode)");
        registerProcedures(GraphCreateProc.class, StreamProc.class, WriteProc.class, MutateProc.class);
    }

    @Test
    void stream() {
        var query = GdsCypher.call()
            .withNodeLabel("RealNode")
            .withAnyRelationshipType()
            .algo("example", "pregel", "test")
            .streamMode()
            .addParameter("maxIterations", 20)
            .yields("nodeId", "values");

        assertCypherResult(query, List.of(Map.of(
            "nodeId", 1L,
            "values", ConditionFactory.containsExactlyInAnyOrderEntriesOf(Map.of(
                CompositeTestAlgorithm.LONG_KEY, 42L,
                CompositeTestAlgorithm.DOUBLE_KEY, 42D,
                CompositeTestAlgorithm.LONG_ARRAY_KEY, new long[]{1, 3, 3, 7},
                CompositeTestAlgorithm.DOUBLE_ARRAY_KEY, new double[]{1, 9, 8, 4}
            ))
        )));
    }

    @ParameterizedTest
    @EnumSource(Partitioning.class)
    void streamWithPartitioning(Partitioning partitioningScheme) {
        var query = GdsCypher.call()
            .withNodeLabel("RealNode")
            .withAnyRelationshipType()
            .algo("example", "pregel", "test")
            .streamMode()
            .addParameter("maxIterations", 20)
            .addParameter("partitioning", partitioningScheme.toString().toLowerCase(Locale.ENGLISH))
            .yields("nodeId", "values");

        assertCypherResult(query, List.of(Map.of(
            "nodeId", 1L,
            "values", ConditionFactory.containsExactlyInAnyOrderEntriesOf(Map.of(
                CompositeTestAlgorithm.LONG_KEY, 42L,
                CompositeTestAlgorithm.DOUBLE_KEY, 42D,
                CompositeTestAlgorithm.LONG_ARRAY_KEY, new long[]{1, 3, 3, 7},
                CompositeTestAlgorithm.DOUBLE_ARRAY_KEY, new double[]{1, 9, 8, 4}
            ))
        )));
    }

    @Test
    void streamWithInvalidPartitioning() {
        var query = GdsCypher.call()
            .withNodeLabel("RealNode")
            .withAnyRelationshipType()
            .algo("example", "pregel", "test")
            .streamMode()
            .addParameter("maxIterations", 20)
            .addParameter("partitioning", "perfect")
            .yields("nodeId", "values");

        assertThatThrownBy(() -> {
            runQuery(query);
        })
            .getRootCause()
            .hasMessageContaining("Partitioning with name `PERFECT` does not exist. Available options are ['AUTO', 'DEGREE', 'RANGE'].");
    }

    @Test
    void write() {
        var writePrefix = "test_";
        var query = GdsCypher.call()
            .withNodeLabel("RealNode")
            .withAnyRelationshipType()
            .algo("example", "pregel", "test")
            .writeMode()
            .addParameter("maxIterations", 20)
            .addParameter("writeProperty", writePrefix)
            .yields();

        runQuery(query);

        var validationQuery = formatWithLocale(
            "MATCH (n:RealNode) RETURN n.%6$s%s AS long, n.%6$s%s AS double, n.%6$s%s AS long_array, n.%6$s%s AS double_array, exists(n.%6$s%s) AS exists",
            CompositeTestAlgorithm.LONG_KEY,
            CompositeTestAlgorithm.DOUBLE_KEY,
            CompositeTestAlgorithm.LONG_ARRAY_KEY,
            CompositeTestAlgorithm.DOUBLE_ARRAY_KEY,
            CompositeTestAlgorithm.PRIVATE_LONG_KEY,
            writePrefix
        );

        assertCypherResult(validationQuery, List.of(
            Map.of(
                "long", 42L,
                "double", 42.0D,
                "long_array", new long[] {1, 3, 3, 7},
                "double_array", new double[] {1, 9, 8, 4},
                "exists", false
            )
        ));
    }

    @Test
    void mutate() {
        var graphName = "testGraph";
        var mutatePrefix = "test_";

        var loadQuery = GdsCypher.call()
            .withNodeLabel("RealNode")
            .withAnyRelationshipType()
            .graphCreate(graphName)
            .yields();

        runQuery(loadQuery);

        var query = GdsCypher.call()
            .explicitCreation(graphName)
            .algo("example", "pregel", "test")
            .mutateMode()
            .addParameter("maxIterations", 20)
            .addParameter("mutateProperty", mutatePrefix)
            .yields();

        runQuery(query);

        var graph = GraphStoreCatalog.get(getUsername(), db.databaseId(), graphName).graphStore().getUnion();

        assertGraphEquals(
            fromGdl(
                "(:RealNode { test_long: 42L, test_double: 42.0D, test_long_array: [1L, 3L, 3L, 7L], test_double_array: [1.0, 9.0, 8.0, 4.0] })"),
            graph
        );
    }

    @Test
    void cleanupEventTrackerWhenTheAlgorithmFailsInStreamMode() {
        var eventTracker = new TestProgressEventTracker();
        try (var transactions = newKernelTransaction(db)) {
            var proc = new StreamProc();
            proc.progressEventTracker = eventTracker;
            proc.api = db;
            proc.transaction = transactions.ktx();
            proc.procedureTransaction = transactions.tx();
            proc.log = NullLog.getInstance();
            Map<String, Object> config = Map.of(
                "nodeProjection", "RealNode",
                "relationshipProjection", "*",
                "maxIterations", 20,
                "throwInCompute", true
            );

            assertThatThrownBy(() -> proc.stream(config, Map.of())).isNotNull();
            // TODO: set to 1 once tracking is implemented
            assertThat(eventTracker.releaseCalls()).isEqualTo(0);
        }
    }

    @Test
    void cleanupEventTrackerWhenTheAlgorithmFailsInWriteMode() {
        var eventTracker = new TestProgressEventTracker();
        try (var transactions = newKernelTransaction(db)) {
            var proc = new WriteProc();
            proc.progressEventTracker = eventTracker;
            proc.api = db;
            proc.transaction = transactions.ktx();
            proc.procedureTransaction = transactions.tx();
            proc.log = NullLog.getInstance();
            Map<String, Object> config = Map.of(
                "nodeProjection", "RealNode",
                "relationshipProjection", "*",
                "maxIterations", 20,
                "throwInCompute", true
            );

            assertThatThrownBy(() -> proc.write(config, Map.of())).isNotNull();
            // TODO: set to 1 once tracking is implemented
            assertThat(eventTracker.releaseCalls()).isEqualTo(0);
        }
    }

    @Test
    void cleanupEventTrackerWhenTheAlgorithmFailsInMutateMode() {
        var graphName = "testGraph";
        runQuery(GdsCypher.call().withNodeLabel("RealNode").withAnyRelationshipType().graphCreate(graphName).yields());

        var eventTracker = new TestProgressEventTracker();
        try (var transactions = newKernelTransaction(db)) {
            var proc = new MutateProc();
            proc.progressEventTracker = eventTracker;
            proc.api = db;
            proc.transaction = transactions.ktx();
            proc.procedureTransaction = transactions.tx();
            proc.log = NullLog.getInstance();
            Map<String, Object> config = Map.of(
                "maxIterations", 20,
                "throwInCompute", true
            );

            assertThatThrownBy(() -> proc.mutate(graphName, config)).isNotNull();
            // TODO: set to 1 once tracking is implemented
            assertThat(eventTracker.releaseCalls()).isEqualTo(0);
        }
    }

    public static class MutateProc extends PregelMutateProc<CompositeTestAlgorithm, TestPregelConfig> {

        @Procedure(
            name = "example.pregel.test.mutate",
            mode = Mode.READ
        )
        @Description("Connected Components")
        public Stream<PregelMutateResult> mutate(
            @Name("graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            return mutate(compute(graphNameOrConfig, configuration));
        }

        @Override
        protected AbstractResultBuilder<PregelMutateResult> resultBuilder(ComputationResult<CompositeTestAlgorithm, PregelResult, TestPregelConfig> computeResult) {
            var ranIterations = computeResult.result().ranIterations();
            var didConverge = computeResult.result().didConverge();
            return new PregelMutateResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
        }

        @Override
        protected TestPregelConfig newConfig(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
        ) {
            return TestPregelConfig.of(username, graphName, maybeImplicitCreate, config);
        }

        @Override
        protected AlgorithmFactory<CompositeTestAlgorithm, TestPregelConfig> algorithmFactory() {
            return new AlgorithmFactory<>() {

                @Override
                protected String taskName() {
                    return "CompositeTestAlgorithm";
                }

                @Override
                protected CompositeTestAlgorithm build(
                    Graph graph,
                    TestPregelConfig configuration,
                    AllocationTracker tracker,
                    ProgressTracker progressTracker
                ) {
                    return new CompositeTestAlgorithm(
                        graph,
                        configuration,
                        tracker,
                        log,
                        configuration.throwInCompute()
                    );
                }

                @Override
                public MemoryEstimation memoryEstimation(TestPregelConfig configuration) {
                    return MemoryEstimations.empty();
                }
            };
        }
    }

    public static class WriteProc extends PregelWriteProc<CompositeTestAlgorithm, TestPregelConfig> {

        @Procedure(
            name = "example.pregel.test.write",
            mode = Mode.WRITE
        )
        @Description("Connected Components")
        public Stream<PregelWriteResult> write(
            @Name("graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            return write(compute(graphNameOrConfig, configuration));
        }

        @Override
        protected AbstractResultBuilder<PregelWriteResult> resultBuilder(ComputationResult<CompositeTestAlgorithm, PregelResult, TestPregelConfig> computeResult) {
            var ranIterations = computeResult.result().ranIterations();
            var didConverge = computeResult.result().didConverge();
            return new PregelWriteResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
        }

        @Override
        protected TestPregelConfig newConfig(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
        ) {
            return TestPregelConfig.of(username, graphName, maybeImplicitCreate, config);
        }

        @Override
        protected AlgorithmFactory<CompositeTestAlgorithm, TestPregelConfig> algorithmFactory() {
            return new AlgorithmFactory<>() {

                @Override
                protected String taskName() {
                    return "CompositeTestAlgorithm";
                }

                @Override
                protected CompositeTestAlgorithm build(
                    Graph graph,
                    TestPregelConfig configuration,
                    AllocationTracker tracker,
                    ProgressTracker progressTracker
                ) {
                    return new CompositeTestAlgorithm(
                        graph,
                        configuration,
                        tracker,
                        log,
                        configuration.throwInCompute()
                    );
                }

                @Override
                public MemoryEstimation memoryEstimation(TestPregelConfig configuration) {
                    return MemoryEstimations.empty();
                }
            };
        }
    }

    public static class StreamProc extends PregelStreamProc<CompositeTestAlgorithm, TestPregelConfig> {

        @Procedure(
            name = "example.pregel.test.stream",
            mode = Mode.READ
        )
        public Stream<PregelStreamResult> stream(
            @Name("graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            return stream(compute(graphNameOrConfig, configuration));
        }

        @Override
        protected PregelStreamResult streamResult(
            long originalNodeId, long internalNodeId, NodeProperties nodeProperties
        ) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected TestPregelConfig newConfig(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
        ) {
            return TestPregelConfig.of(username, graphName, maybeImplicitCreate, config);
        }

        @Override
        protected AlgorithmFactory<CompositeTestAlgorithm, TestPregelConfig> algorithmFactory() {
            return new AlgorithmFactory<>() {

                @Override
                protected String taskName() {
                    return "CompositeTestAlgorithm";
                }

                @Override
                protected CompositeTestAlgorithm build(
                    Graph graph,
                    TestPregelConfig configuration,
                    AllocationTracker tracker,
                    ProgressTracker progressTracker
                ) {
                    return new CompositeTestAlgorithm(
                        graph,
                        configuration,
                        tracker,
                        log,
                        configuration.throwInCompute()
                    );
                }

                @Override
                public MemoryEstimation memoryEstimation(TestPregelConfig configuration) {
                    return MemoryEstimations.empty();
                }
            };
        }
    }

    public static class CompositeTestAlgorithm extends Algorithm<CompositeTestAlgorithm, PregelResult> {

        static final String LONG_KEY = "long";
        static final String DOUBLE_KEY = "double";
        static final String LONG_ARRAY_KEY = "long_array";
        static final String DOUBLE_ARRAY_KEY = "double_array";
        static final String PRIVATE_LONG_KEY = "long_private";

        private final Pregel<PregelProcedureConfig> pregelJob;

        CompositeTestAlgorithm(
            Graph graph,
            PregelProcedureConfig configuration,
            AllocationTracker tracker,
            Log log,
            boolean throwInCompute
        ) {
            var progressLogger = new BatchingProgressLogger(log, Tasks.leaf("test", 42), 1);
            this.pregelJob = Pregel.create(graph, configuration, new PregelComputation<>() {

                @Override
                public PregelSchema schema(PregelProcedureConfig config) {
                    return new PregelSchema.Builder()
                        .add(LONG_KEY, ValueType.LONG)
                        .add(DOUBLE_KEY, ValueType.DOUBLE)
                        .add(LONG_ARRAY_KEY, ValueType.LONG_ARRAY)
                        .add(DOUBLE_ARRAY_KEY, ValueType.DOUBLE_ARRAY)
                        .add(PRIVATE_LONG_KEY, ValueType.LONG, PregelSchema.Visibility.PRIVATE)
                        .build();
                }

                @Override
                public void compute(ComputeContext<PregelProcedureConfig> context, Messages messages) {
                    if (throwInCompute) {
                        throw new IllegalStateException("boo");
                    }
                    context.setNodeValue(LONG_KEY, 42L);
                    context.setNodeValue(DOUBLE_KEY, 42.0D);
                    context.setNodeValue(LONG_ARRAY_KEY, new long[]{1, 3, 3, 7});
                    context.setNodeValue(DOUBLE_ARRAY_KEY, new double[]{1, 9, 8, 4});
                }
            }, Pools.DEFAULT, tracker, progressLogger);
        }

        @Override
        public PregelResult compute() {
            return pregelJob.run();
        }

        @Override
        public CompositeTestAlgorithm me() {
            return this;
        }

        @Override
        public void release() {

        }
    }
}
