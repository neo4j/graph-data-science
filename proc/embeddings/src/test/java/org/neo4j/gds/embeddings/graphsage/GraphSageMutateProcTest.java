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
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.StoreLoaderWithConfigBuilder;
import org.neo4j.gds.api.GraphStore;
import org.neo4j.gds.api.properties.nodes.NodePropertyValues;
import org.neo4j.gds.config.GraphProjectFromStoreConfig;
import org.neo4j.gds.core.loading.GraphStoreCatalog;
import org.neo4j.gds.utils.StringJoining;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GraphSageMutateProcTest extends GraphSageBaseProcTest {

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.graphsage.GraphSageBaseProcTest#configVariations")
    void testWriting(int embeddingSize, String aggregator, ActivationFunction activationFunction) {
        train(embeddingSize, aggregator, activationFunction);

        String mutatePropertyKey = "embedding";
        String query = GdsCypher.call("embeddingsGraph")
            .algo("gds.beta.graphSage")
            .mutateMode()
            .addParameter("mutateProperty", mutatePropertyKey)
            .addParameter("modelName", modelName)
            .yields();

        GraphStore graphStore = GraphStoreCatalog.get(getUsername(), db.databaseId(), graphName).graphStore();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("nodeCount")).isEqualTo(graphStore.nodeCount());
            assertThat(row.get("nodePropertiesWritten")).isEqualTo(graphStore.nodeCount());
            assertThat(row.get("preProcessingMillis")).isNotEqualTo(-1L);
            assertThat(row.get("computeMillis")).isNotEqualTo(-1L);
            assertThat(row.get("mutateMillis")).isNotEqualTo(-1L);
            assertThat(row.get("configuration")).isInstanceOf(Map.class);
        });

        NodePropertyValues embeddingNodePropertyValues = graphStore.nodeProperty(mutatePropertyKey).values();
        graphStore.nodes().forEachNode(nodeId -> {
            assertEquals(embeddingSize, embeddingNodePropertyValues.doubleArrayValue(nodeId).length);
            return true;
        });
    }

    @ParameterizedTest(name = "Graph Properties: {2} - Algo Properties: {1}")
    @MethodSource("missingNodeProperties")
    void shouldFailOnMissingNodeProperties(
        GraphProjectFromStoreConfig config,
        List<String> nodeProperties,
        List<String> graphProperties,
        List<String> label
    ) {
        train(16, "mean", ActivationFunction.SIGMOID);

        var graphStore = new StoreLoaderWithConfigBuilder()
            .databaseService(db)
            .graphProjectConfig(config)
            .build()
            .graphStore();
        GraphStoreCatalog.set(config, graphStore);

        String query = GdsCypher.call(config.graphName())
            .algo("gds.beta.graphSage")
            .mutateMode()
            .addParameter("concurrency", 1)
            .addParameter("modelName", modelName)
            .addParameter("mutateProperty", modelName)
            .yields();

        assertThatThrownBy(() -> runQuery(query))
            .isInstanceOf(QueryExecutionException.class)
            .hasRootCauseInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("The feature properties %s are not present for any of the requested labels.", StringJoining.join(nodeProperties))
            .hasMessageContaining("Requested labels: %s", StringJoining.join(label))
            .hasMessageContaining("Properties available on the requested labels: %s", StringJoining.join(graphProperties));
    }

}
