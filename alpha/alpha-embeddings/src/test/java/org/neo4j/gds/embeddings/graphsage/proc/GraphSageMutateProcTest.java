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
package org.neo4j.gds.embeddings.graphsage.proc;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.api.GraphStore;
import org.neo4j.graphalgo.api.NodeProperties;
import org.neo4j.graphalgo.config.GraphCreateFromStoreConfig;
import org.neo4j.graphalgo.core.loading.GraphStoreCatalog;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.graphalgo.utils.ExceptionUtil.rootCause;
import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

class GraphSageMutateProcTest extends GraphSageBaseProcTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.graphsage.proc.GraphSageBaseProcTest#configVariations")
    void testWriting(int embeddingSize, String aggregator, ActivationFunction activationFunction) {
        train(embeddingSize, aggregator, activationFunction);

        String mutatePropertyKey = "embedding";
        String query = GdsCypher.call().explicitCreation("embeddingsGraph")
            .algo("gds.alpha.graphSage")
            .mutateMode()
            .addParameter("mutateProperty", mutatePropertyKey)
            .addParameter("modelName", modelName)
            .yields();

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), graphName).graphStore();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("nodeCount")).isEqualTo(graphStore.nodeCount());
            assertThat(row.get("nodePropertiesWritten")).isEqualTo(graphStore.nodeCount());
            assertThat(row.get("createMillis")).isNotEqualTo(-1L);
            assertThat(row.get("computeMillis")).isNotEqualTo(-1L);
            assertThat(row.get("mutateMillis")).isNotEqualTo(-1L);
            assertThat(row.get("configuration")).isInstanceOf(Map.class);
        });

        NodeProperties embeddingNodeProperties = graphStore.nodePropertyValues(mutatePropertyKey);
        graphStore.nodes().forEachNode(nodeId -> {
            assertEquals(embeddingSize, embeddingNodeProperties.doubleArrayValue(nodeId).length);
            return true;
        });
    }

    @ParameterizedTest(name = "Graph Properties: {2} - Algo Properties: {1}")
    @MethodSource("missingNodeProperties")
    void shouldFailOnMissingNodeProperties(GraphCreateFromStoreConfig config, String nodeProperties, String graphProperties, String label) {
        train(16, "mean", ActivationFunction.SIGMOID);

        String query = GdsCypher.call().implicitCreation(config)
            .algo("gds.alpha.graphSage")
            .mutateMode()
            .addParameter("concurrency", 1)
            .addParameter("modelName", modelName)
            .addParameter("mutateProperty", modelName)
            .yields();

        String expectedFail = formatWithLocale("Node properties [%s] not found in graph with node properties: [%s] in all node labels: ['%s']", nodeProperties, graphProperties, label);
        Throwable throwable = rootCause(assertThrows(QueryExecutionException.class, () -> runQuery(query)));
        assertEquals(IllegalArgumentException.class, throwable.getClass());
        assertEquals(expectedFail, throwable.getMessage());
    }

}