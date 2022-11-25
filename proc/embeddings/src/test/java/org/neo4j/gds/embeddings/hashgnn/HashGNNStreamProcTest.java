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
package org.neo4j.gds.embeddings.hashgnn;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

@SuppressWarnings("unchecked")
class HashGNNStreamProcTest extends HashGNNProcTest<HashGNNStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<HashGNN, HashGNN.HashGNNResult, HashGNNStreamConfig, ?>> getProcedureClazz() {
        return HashGNNStreamProc.class;
    }

    @Override
    public HashGNNStreamConfig createConfig(CypherMapWrapper userInput) {
        return HashGNNStreamConfig.of(userInput);
    }

    @Test
    void shouldComputeNonZeroEmbeddings() {
        String GRAPH_NAME = "myGraph";

        String graphCreateQuery = GdsCypher.call(GRAPH_NAME)
            .graphProject()
            .withNodeLabel("N")
            .withAnyRelationshipType()
            .withNodeProperty("f1")
            .withNodeProperty("f2")
            .yields();
        runQuery(graphCreateQuery);

        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call(GRAPH_NAME)
            .algo("gds.alpha.hashgnn")
            .streamMode()
            .addParameter("featureProperties", List.of("f1", "f2"))
            .addParameter("embeddingDensity", 2)
            .addParameter("iterations", 10);

        String query = queryBuilder.yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat((List<Double>) row.get("embedding"))
                .hasSize(3)
                .anyMatch(value -> value != 0.0);
        });

    }

    @Test
    void failsOnInvalidBinarizationKeys() {
        assertThatThrownBy(() -> {
            HashGNNStreamConfig.of(CypherMapWrapper.create(
                Map.of(
                    "featureProperties", List.of("x"),
                    "binarizeFeatures", Map.of("dimension", 100, "treshold", 2.0),
                    "embeddingDensity", 4,
                    "iterations", 100
                )
            ));

        }).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unexpected configuration key: treshold (Did you mean [threshold]?)");
    }

    @Test
    void failsOnInvalidGenerateFeaturesKeys() {
        assertThatThrownBy(() -> {
            HashGNNStreamConfig.of(CypherMapWrapper.create(
                Map.of(
                    "generateFeatures", Map.of("dimension", 100, "densityElfen", 2),
                    "embeddingDensity", 4,
                    "iterations", 100
                )
            ));

        }).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("No value specified for the mandatory configuration parameter `densityLevel` (a similar parameter exists: [densityElfen])");
    }
}
