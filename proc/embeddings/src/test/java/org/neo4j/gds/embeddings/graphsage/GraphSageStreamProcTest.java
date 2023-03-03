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
package org.neo4j.gds.embeddings.graphsage;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.utils.StringJoining;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GraphSageStreamProcTest extends GraphSageBaseProcTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.graphsage.GraphSageBaseProcTest#configVariations")
    void testStreaming(int embeddingDimension, String aggregator, ActivationFunction activationFunction) {
        train(embeddingDimension, aggregator, activationFunction);

        String query = GdsCypher.call("embeddingsGraph")
            .algo("gds.beta.graphSage")
            .streamMode()
            .addParameter("concurrency", 1)
            .addParameter("modelName", modelName)
            .yields();

        runQueryWithRowConsumer(query, Map.of("embeddingDimension", embeddingDimension), row -> {
            assertThat(row.getNumber("nodeId"))
                .isNotNull();

            assertThat(row.get("embedding"))
                .asList()
                .hasSize(embeddingDimension);
        });
    }

    @Test
    void weightedGraphSage() {
        var trainQuery = GdsCypher.call(graphName)
            .algo("gds.beta.graphSage")
            .trainMode()
            .addParameter("sampleSizes", List.of(1))
            .addParameter("maxIterations", 1)
            .addParameter("featureProperties", List.of("age"))
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("embeddingDimension", 1)
            .addParameter("activationFunction", "RELU")
            .addParameter("aggregator", "MEAN")
            .addParameter("randomSeed", 42L)
            .addParameter("modelName", modelName)
            .yields();

        runQuery(trainQuery);

        String streamQuery = GdsCypher.call(graphName)
            .algo("gds.beta.graphSage")
            .streamMode()
            .addParameter("concurrency", 1)
            .addParameter("modelName", modelName)
            .yields();

        assertCypherResult(streamQuery, List.of(
            Map.of("nodeId", 0L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 1L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 2L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 3L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 4L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 5L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 6L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 7L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 8L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 9L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 10L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 11L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 12L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 13L, "embedding", Matchers.iterableWithSize(1)),
            Map.of("nodeId", 14L, "embedding", Matchers.iterableWithSize(1))
        ));
    }

    @ParameterizedTest(name = "Graph Properties: {2} - Algo Properties: {1}")
    @MethodSource("missingNodeProperties")
    void shouldFailOnMissingNodeProperties(
        GraphProjectFromStoreConfig config,
        List<String> nodeProperties,
        List<String> graphProperties,
        List<String> label
    ) {
        train(42, "mean", ActivationFunction.SIGMOID);

        runQuery(GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withGraphProjectConfig(config)
            .yields()
        );

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("gds.beta.graphSage")
            .streamMode()
            .addParameter("concurrency", 1)
            .addParameter("modelName", modelName)
            .yields();

        assertThatThrownBy(() -> runQuery(query))
            .isInstanceOf(QueryExecutionException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The feature properties %s are not present for all requested labels.", StringJoining.join(nodeProperties))
            .hasMessageContaining("Requested labels: %s", StringJoining.join(label))
            .hasMessageContaining("Properties available on all requested labels: %s", StringJoining.join(graphProperties));
    }
}
