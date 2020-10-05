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
import org.neo4j.graphalgo.config.GraphCreateConfig;
import org.neo4j.graphalgo.core.CypherMapWrapper;
import org.neo4j.graphalgo.core.concurrency.Pools;
import org.neo4j.graphalgo.core.utils.mem.AllocationTracker;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimation;
import org.neo4j.graphalgo.core.utils.mem.MemoryEstimations;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PregelProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE (a)";

    @BeforeEach
    void setup() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(StreamProc.class);
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
            assertArrayEquals(new long[0], (long[]) values.get(CompositeTestAlgorithm.LONG_ARRAY_KEY));
            assertArrayEquals(new double[0], (double[]) values.get(CompositeTestAlgorithm.DOUBLE_ARRAY_KEY));
            assertTrue(Double.isNaN((double) values.get(CompositeTestAlgorithm.DOUBLE_KEY)));
            assertEquals(Long.MIN_VALUE, values.get(CompositeTestAlgorithm.LONG_KEY));
        });
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
                public void compute(PregelContext.ComputeContext<PregelConfig> context, Pregel.Messages messages) {}
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
