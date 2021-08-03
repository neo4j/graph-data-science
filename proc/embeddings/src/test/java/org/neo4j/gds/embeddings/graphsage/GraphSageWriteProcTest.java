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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.utils.StringJoining;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;

class GraphSageWriteProcTest extends GraphSageBaseProcTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.graphsage.GraphSageBaseProcTest#configVariations")
    void testWriting(int embeddingDimension, String aggregator, ActivationFunction activationFunction) {
        train(embeddingDimension, aggregator, activationFunction);

        String query = GdsCypher.call().explicitCreation("embeddingsGraph")
            .algo("gds.beta.graphSage")
            .writeMode()
            .addParameter("writeProperty", "embedding")
            .addParameter("modelName", modelName)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            assertNotNull(row.get("nodeCount"));
            assertNotNull(row.get("nodePropertiesWritten"));
            assertNotNull(row.get("createMillis"));
            assertNotNull(row.get("computeMillis"));
            assertNotNull(row.get("writeMillis"));
            assertNotNull(row.get("configuration"));
        });

        List<Map<String, Object>> results = new ArrayList<>();
        for (int i = 0; i < 15; i++) {
            results.add(Map.of("size", (long)embeddingDimension));
        }

        assertCypherResult(
            "MATCH (n:King) RETURN size(n.embedding) AS size",
            results
        );
    }


    @ParameterizedTest(name = "Graph Properties: {2} - Algo Properties: {1}")
    @MethodSource("missingNodeProperties")
    void shouldFailOnMissingNodeProperties(GraphCreateFromStoreConfig config, List<String> nodeProperties, List<String> graphProperties, List<String> label) {
        train(42, "mean", ActivationFunction.SIGMOID);

        String query = GdsCypher.call().implicitCreation(config)
            .algo("gds.beta.graphSage")
            .writeMode()
            .addParameter("concurrency", 1)
            .addParameter("modelName", modelName)
            .addParameter("writeProperty", modelName)
            .yields();

        String expectedFail = formatWithLocale(
            "The feature properties %s are not present for all requested labels. Requested labels: %s. Properties available on all requested labels: %s",
            StringJoining.join(nodeProperties),
            StringJoining.join(label),
            StringJoining.join(graphProperties)
        );
        Throwable throwable = rootCause(assertThrows(QueryExecutionException.class, () -> runQuery(query)));
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        assertEquals(expectedFail, throwable.getMessage());
    }
}
