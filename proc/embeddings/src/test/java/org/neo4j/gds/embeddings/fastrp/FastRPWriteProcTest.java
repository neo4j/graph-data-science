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
package org.neo4j.gds.embeddings.fastrp;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.core.CypherMapWrapper;
import org.neo4j.gds.test.config.WritePropertyConfigProcTest;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.FLOAT_ARRAY;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.anyMatch;
import static org.neo4j.gds.ml.core.tensor.operations.FloatVectorOperations.scale;

class FastRPWriteProcTest extends FastRPProcTest<FastRPWriteConfig> {

    @TestFactory
    Stream<DynamicTest> configTests() {
        return Stream.of(
            WritePropertyConfigProcTest.test(proc(), createMinimalConfig())
        ).flatMap(Collection::stream);
    }

    @Override
    GdsCypher.ExecutionModes mode() {
        return GdsCypher.ExecutionModes.WRITE;
    }

    @Override
    public Class<? extends AlgoBaseProc<FastRP, FastRP.FastRPResult, FastRPWriteConfig, ?>> getProcedureClazz() {
        return FastRPWriteProc.class;
    }

    @Override
    public FastRPWriteConfig createConfig(CypherMapWrapper userInput) {
        return FastRPWriteConfig.of(userInput);
    }

    @Override
    public CypherMapWrapper createMinimalConfig(CypherMapWrapper userInput) {
        CypherMapWrapper minimalConfig = super.createMinimalConfig(userInput);

        if (!minimalConfig.containsKey("writeProperty")) {
            return minimalConfig.withString("writeProperty", "embedding");
        }
        return minimalConfig;
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.fastrp.FastRPProcTest#weights")
    void shouldComputeNonZeroEmbeddings(List<Float> weights) {
        List<String> featureProperties = List.of("f1", "f2");
        var propertyRatio = 0.5;
        int embeddingDimension = 128;
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call(FASTRP_GRAPH)
            .algo("fastRP")
            .writeMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("propertyRatio", propertyRatio)
            .addParameter("featureProperties", featureProperties)
            .addParameter("writeProperty", "embedding");

        if (!weights.isEmpty()) {
            queryBuilder.addParameter("iterationWeights", weights);
        }
        String writeQuery = queryBuilder.yields();

        runQuery(writeQuery);

        runQueryWithRowConsumer("MATCH (n:Node) RETURN n.embedding as embedding", row -> {
            assertThat(row.get("embedding"))
                .asInstanceOf(FLOAT_ARRAY)
                .hasSize(embeddingDimension)
                .matches(vector -> anyMatch(vector, v -> v != 0.0));
        });
    }

    @Test
    void shouldComputeAndWriteWithWeight() {
        var createQuery = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .graphProject()
            .withNodeLabel("Node")
            .withNodeLabel("Node2")
            .withNodeProperties(List.of("f1", "f2"), DefaultValue.of(0.0f))
            .withRelationshipType("REL2")
            .withRelationshipProperty("weight")
            .yields();
        runQuery(createQuery);

        List<String> featureProperties = List.of("f1", "f2");
        var propertyRatio = 0.5;
        int embeddingDimension = 128;

        String query = GdsCypher.call(DEFAULT_GRAPH_NAME)
            .algo("fastRP")
            .writeMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("propertyRatio", propertyRatio)
            .addParameter("featureProperties", featureProperties)
            .addParameter("relationshipWeightProperty", "weight")
            .addParameter("writeProperty", "embedding")
            .yields();

        runQuery(query);

        String retrieveQuery = "MATCH (n) WHERE n:Node OR n:Node2 RETURN n.name as name, n.embedding as embedding";
        Map<String, float[]> embeddings = new HashMap<>(3);
        runQueryWithRowConsumer(retrieveQuery, row -> {
            embeddings.put(row.getString("name"), (float[]) row.get("embedding"));
        });

        float[] embeddingOfE = embeddings.get("e");
        scale(embeddingOfE, 2);
        assertThat(embeddings.get("b")).containsExactly(embeddingOfE);
    }
}
