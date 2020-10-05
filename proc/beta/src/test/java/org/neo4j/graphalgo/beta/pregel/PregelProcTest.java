/*
 * Copyright (c) 2017-2020 "Neo4j,"
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
package org.neo4j.graphalgo.beta.pregel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.Algorithm;
import org.neo4j.graphalgo.AlgorithmFactory;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.api.nodeproperties.ValueType;
import org.neo4j.graphalgo.catalog.GraphCreateProc;
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.graphalgo.result.AbstractResultBuilder;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphalgo.TestSupport.assertGraphEquals;
import static org.neo4j.graphalgo.TestSupport.fromGdl;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public class PregelProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE (a)";

    @BeforeEach
    void setup() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(GraphCreateProc.class, StreamProc.class, WriteProc.class, MutateProc.class);
    }

    @Test
    void stream() {
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "test")
            .streamMode()
            .addParameter("maxIterations", 20)
            .yields("nodeId", "values");

        runQueryWithRowConsumer(query, row -> {
            var values = (Map<String, Object>) row.get("values");
            assertEquals(42L, values.get(CompositeTestAlgorithm.LONG_KEY));
            assertEquals(42.0D, values.get(CompositeTestAlgorithm.DOUBLE_KEY));
            assertArrayEquals(new long[]{1, 3, 3, 7}, (long[]) values.get(CompositeTestAlgorithm.LONG_ARRAY_KEY));
            assertArrayEquals(new double[]{1, 9, 8, 4}, (double[]) values.get(CompositeTestAlgorithm.DOUBLE_ARRAY_KEY));
        });
    }

    @Test
    void write() {
        var writePrefix = "test_";
        var query = GdsCypher.call()
            .loadEverything()
            .algo("example", "pregel", "test")
            .writeMode()
            .addParameter("maxIterations", 20)
            .addParameter("writeProperty", writePrefix)
            .yields();

        runQuery(query);

        var validationQuery = formatWithLocale(
            "MATCH (n) RETURN n.%s AS long, n.%s AS double, n.%s AS long_array, n.%s AS double_array",
            writePrefix + CompositeTestAlgorithm.LONG_KEY,
            writePrefix + CompositeTestAlgorithm.DOUBLE_KEY,
            writePrefix + CompositeTestAlgorithm.LONG_ARRAY_KEY,
            writePrefix + CompositeTestAlgorithm.DOUBLE_ARRAY_KEY
        );

        assertCypherResult(validationQuery, List.of(
            Map.of(
                "long", 42L,
                "double", 42.0D,
                "long_array", new long[] {1, 3, 3, 7},
                "double_array", new double[] {1, 9, 8, 4}
            )
        ));
    }

    @Test
    void mutate() {
        var graphName = "testGraph";
        var mutatePrefix = "test_";

        var loadQuery = GdsCypher.call()
            .loadEverything()
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
                "(a { test_long: 42L, test_double: 42.0D, test_long_array: [1L, 3L, 3L, 7L], test_double_array: [1.0, 9.0, 8.0, 4.0] })"),
            graph
        );
    }

    public static class MutateProc extends PregelMutateProc<CompositeTestAlgorithm, PregelConfig> {

        @Procedure(
            name = "example.pregel.test.mutate",
            mode = Mode.WRITE
        )
        @Description("Connected Components")
        public Stream<PregelMutateResult> mutate(
            @Name("graphName") Object graphNameOrConfig,
            @Name(value = "configuration", defaultValue = "{}") Map<String, Object> configuration
        ) {
            return mutate(compute(graphNameOrConfig, configuration));
        }

        @Override
        protected AbstractResultBuilder<PregelMutateResult> resultBuilder(ComputationResult<CompositeTestAlgorithm, Pregel.PregelResult, PregelConfig> computeResult) {
            var ranIterations = computeResult.result().ranIterations();
            var didConverge = computeResult.result().didConverge();
            return new PregelMutateResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
        }

        @Override
        protected PregelConfig newConfig(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
        ) {
            return PregelConfig.of(username, graphName, maybeImplicitCreate, config);
        }

        @Override
        protected AlgorithmFactory<CompositeTestAlgorithm, PregelConfig> algorithmFactory() {
            return new AlgorithmFactory<>() {
                @Override
                public CompositeTestAlgorithm build(
                    Graph graph,
                    PregelConfig configuration, AllocationTracker tracker, Log log
                ) {
                    return new CompositeTestAlgorithm(graph, configuration, tracker, log);
                }

                @Override
                public MemoryEstimation memoryEstimation(PregelConfig configuration) {
                    return MemoryEstimations.empty();
                }
            };
        }
    }

    public static class WriteProc extends PregelWriteProc<CompositeTestAlgorithm, PregelConfig> {

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
        protected AbstractResultBuilder<PregelWriteResult> resultBuilder(ComputationResult<CompositeTestAlgorithm, Pregel.PregelResult, PregelConfig> computeResult) {
            var ranIterations = computeResult.result().ranIterations();
            var didConverge = computeResult.result().didConverge();
            return new PregelWriteResult.Builder().withRanIterations(ranIterations).didConverge(didConverge);
        }

        @Override
        protected PregelConfig newConfig(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
        ) {
            return PregelConfig.of(username, graphName, maybeImplicitCreate, config);
        }

        @Override
        protected AlgorithmFactory<CompositeTestAlgorithm, PregelConfig> algorithmFactory() {
            return new AlgorithmFactory<>() {
                @Override
                public CompositeTestAlgorithm build(
                    Graph graph,
                    PregelConfig configuration, AllocationTracker tracker, Log log
                ) {
                    return new CompositeTestAlgorithm(graph, configuration, tracker, log);
                }

                @Override
                public MemoryEstimation memoryEstimation(PregelConfig configuration) {
                    return MemoryEstimations.empty();
                }
            };
        }
    }

    public static class StreamProc extends PregelStreamProc<CompositeTestAlgorithm, PregelConfig> {

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
        protected PregelConfig newConfig(
            String username,
            Optional<String> graphName,
            Optional<GraphCreateConfig> maybeImplicitCreate,
            CypherMapWrapper config
        ) {
            return PregelConfig.of(username, graphName, maybeImplicitCreate, config);
        }

        @Override
        protected AlgorithmFactory<CompositeTestAlgorithm, PregelConfig> algorithmFactory() {
            return new AlgorithmFactory<>() {
                @Override
                public CompositeTestAlgorithm build(
                    Graph graph,
                    PregelConfig configuration, AllocationTracker tracker, Log log
                ) {
                    return new CompositeTestAlgorithm(graph, configuration, tracker, log);
                }

                @Override
                public MemoryEstimation memoryEstimation(PregelConfig configuration) {
                    return MemoryEstimations.empty();
                }
            };
        }
    }

    public static class CompositeTestAlgorithm extends Algorithm<CompositeTestAlgorithm, Pregel.PregelResult> {

        static final String LONG_KEY = "long";
        static final String DOUBLE_KEY = "double";
        static final String LONG_ARRAY_KEY = "long_array";
        static final String DOUBLE_ARRAY_KEY = "double_array";

        private final Pregel<PregelConfig> pregelJob;

        CompositeTestAlgorithm(
            Graph graph, PregelConfig configuration,
            AllocationTracker tracker, Log log
        ) {
            this.pregelJob = Pregel.create(graph, configuration, new PregelComputation<>() {

                @Override
                public Pregel.NodeSchema nodeSchema() {
                    return new NodeSchemaBuilder()
                        .putElement(LONG_KEY, ValueType.LONG)
                        .putElement(DOUBLE_KEY, ValueType.DOUBLE)
                        .putElement(LONG_ARRAY_KEY, ValueType.LONG_ARRAY)
                        .putElement(DOUBLE_ARRAY_KEY, ValueType.DOUBLE_ARRAY)
                        .build();
                }

                @Override
                public void compute(PregelContext.ComputeContext<PregelConfig> context, Pregel.Messages messages) {
                    context.setNodeValue(LONG_KEY, 42L);
                    context.setNodeValue(DOUBLE_KEY, 42.0D);
                    context.setNodeValue(LONG_ARRAY_KEY, new long[]{1, 3, 3, 7});
                    context.setNodeValue(DOUBLE_ARRAY_KEY, new double[]{1, 9, 8, 4});
                }
            }, Pools.DEFAULT, tracker);
        }

        @Override
        public Pregel.PregelResult compute() {
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
