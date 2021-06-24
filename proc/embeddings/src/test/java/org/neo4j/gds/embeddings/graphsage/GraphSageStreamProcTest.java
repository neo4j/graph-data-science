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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.utils.StringJoining;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphSageStreamProcTest extends GraphSageBaseProcTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.graphsage.GraphSageBaseProcTest#configVariations")
    void testStreaming(int embeddingDimension, String aggregator, ActivationFunction activationFunction) {
        train(embeddingDimension, aggregator, activationFunction);

        String query = GdsCypher.call().explicitCreation("embeddingsGraph")
            .algo("gds.beta.graphSage")
            .streamMode()
            .addParameter("concurrency", 1)
            .addParameter("modelName", modelName)
            .yields();

        runQueryWithRowConsumer(query, Map.of("embeddingDimension", embeddingDimension), row -> {
            Number nodeId = row.getNumber("nodeId");
            assertNotNull(nodeId);

            Object o = row.get("embedding");
            assertTrue(o instanceof List);
            Collection<Double> nodeEmbeddings = (List<Double>) o;
            assertEquals(embeddingDimension, nodeEmbeddings.size());
        });
    }

    @Test
    void weightedGraphSage() {
        var trainQuery = GdsCypher.call()
            .explicitCreation(graphName)
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

        String streamQuery = GdsCypher.call().explicitCreation(graphName)
            .algo("gds.beta.graphSage")
            .streamMode()
            .addParameter("concurrency", 1)
            .addParameter("modelName", modelName)
            .yields();

        assertCypherResult(streamQuery, List.of(
            Map.of("nodeId", 0L, "embedding", List.of(0.999999999980722)),
            Map.of("nodeId", 1L, "embedding", List.of(0.9999999999975783)),
            Map.of("nodeId", 2L, "embedding", List.of(0.9999999999880146)),
            Map.of("nodeId", 3L, "embedding", List.of(0.999999999965436)),
            Map.of("nodeId", 4L, "embedding", List.of(0.9999999999983651)),
            Map.of("nodeId", 5L, "embedding", List.of(0.9999999999377848)),
            Map.of("nodeId", 6L, "embedding", List.of(0.9999999999580143)),
            Map.of("nodeId", 7L, "embedding", List.of(0.9999999999420027)),
            Map.of("nodeId", 8L, "embedding", List.of(0.9999999996197955)),
            Map.of("nodeId", 9L, "embedding", List.of(-0.9999999619795564)),
            Map.of("nodeId", 10L, "embedding", List.of(0.9999999999485437)),
            Map.of("nodeId", 11L, "embedding", List.of(-0.9999999979930556)),
            Map.of("nodeId", 12L, "embedding", List.of(0.999999999965436)),
            Map.of("nodeId", 13L, "embedding", List.of(0.999999999965436)),
            Map.of("nodeId", 14L, "embedding", List.of(0.999999999965436))
        ));
    }

    @ParameterizedTest(name = "Graph Properties: {2} - Algo Properties: {1}")
    @MethodSource("missingNodeProperties")
    void shouldFailOnMissingNodeProperties(GraphCreateFromStoreConfig config, List<String> nodeProperties, List<String> graphProperties, List<String> label) {
        train(42, "mean", ActivationFunction.SIGMOID);

        String query = GdsCypher.call().implicitCreation(config)
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
