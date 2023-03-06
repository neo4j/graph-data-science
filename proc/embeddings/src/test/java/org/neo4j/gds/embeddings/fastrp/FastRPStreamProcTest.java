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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.gds.AlgoBaseProc;
import org.neo4j.gds.GdsCypher;
import org.neo4j.gds.api.DefaultValue;
import org.neo4j.gds.core.CypherMapWrapper;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SuppressWarnings("unchecked")
class FastRPStreamProcTest extends FastRPProcTest<FastRPStreamConfig> {

    @Override
    GdsCypher.ExecutionModes mode() {
        return GdsCypher.ExecutionModes.STREAM;
    }

    @Override
    public Class<? extends AlgoBaseProc<FastRP, FastRP.FastRPResult, FastRPStreamConfig, ?>> getProcedureClazz() {
        return FastRPStreamProc.class;
    }

    @Override
    public FastRPStreamConfig createConfig(CypherMapWrapper userInput) {
        return FastRPStreamConfig.of(userInput);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.fastrp.FastRPProcTest#weights")
    void shouldComputeNonZeroEmbeddings(List<Float> weights) {
        List<String> featureProperties = List.of("f1", "f2");
        var propertyRatio = 0.5;
        int embeddingDimension = 128;
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call(FASTRP_GRAPH)
            .algo("fastRP")
            .streamMode()
            .addParameter("propertyRatio", propertyRatio)
            .addParameter("featureProperties", featureProperties)
            .addParameter("embeddingDimension", embeddingDimension);

        if (!weights.isEmpty()) {
            queryBuilder.addParameter("iterationWeights", weights);
        }
        String query = queryBuilder.yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("embedding"))
                .asList()
                .hasSize(embeddingDimension)
                .anySatisfy(value -> assertThat(value).asInstanceOf(DOUBLE).isNotEqualTo(0.0));
        });
    }

    @Test
    void shouldComputeNonZeroEmbeddingsWhenFirstWeightIsZero() {
        List<String> featureProperties = List.of("f1", "f2");
        var propertyRatio = 0.5;
        int embeddingDimension = 128;
        var weights = List.of(0.0D, 1.0D, 2.0D, 4.0D);
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call(FASTRP_GRAPH)
            .algo("fastRP")
            .streamMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("propertyRatio", propertyRatio)
            .addParameter("featureProperties", featureProperties)
            .addParameter("iterationWeights", weights);

        queryBuilder.addParameter("iterationWeights", weights);
        String query = queryBuilder.yields();

        runQueryWithRowConsumer(query, row -> {
            assertThat(row.get("embedding"))
                .asList()
                .anySatisfy(value -> assertThat(value).asInstanceOf(DOUBLE).isNotEqualTo(0.0));
        });
    }

    @Test
    void shouldComputeWithWeight() {
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
            .streamMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("propertyRatio", propertyRatio)
            .addParameter("featureProperties", featureProperties)
            .addParameter("relationshipWeightProperty", "weight")
            .yields();

        List<List<Double>> embeddings = new ArrayList<>(3);
        runQueryWithRowConsumer(query, row -> embeddings.add((List<Double>) row.get("embedding")));

        for (int i = 0; i < 128; i++) {
            assertEquals(embeddings.get(1).get(i), embeddings.get(2).get(i) * 2);
        }
    }
}
