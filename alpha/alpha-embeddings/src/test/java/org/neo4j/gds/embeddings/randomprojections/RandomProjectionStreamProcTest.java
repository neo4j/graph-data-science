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
package org.neo4j.gds.embeddings.randomprojections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RandomProjectionStreamProcTest extends RandomProjectionProcTest<RandomProjectionStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<RandomProjection, RandomProjection, RandomProjectionStreamConfig>> getProcedureClazz() {
        return RandomProjectionStreamProc.class;
    }

    @Override
    public RandomProjectionStreamConfig createConfig(CypherMapWrapper userInput) {
        return RandomProjectionStreamConfig.of(getUsername(), Optional.empty(), Optional.empty(), userInput);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.randomprojections.RandomProjectionProcTest#weights")
    void shouldComputeNonZeroEmbeddings(List<Float> weights) {
        int embeddingSize = 128;
        int maxIterations = 4;
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .algo("gds.alpha.randomProjection")
            .streamMode()
            .addParameter("embeddingSize", embeddingSize)
            .addParameter("maxIterations", maxIterations);

        if (!weights.isEmpty()) {
            queryBuilder.addParameter("iterationWeights", weights);
        }
        String query = queryBuilder.yields();

        int expectedEmbeddingsDimension = weights.isEmpty()
            ? embeddingSize * maxIterations
            : embeddingSize;
        runQueryWithRowConsumer(query, row -> {
            List<Double> embeddings = (List<Double>) row.get("embedding");
            assertEquals(expectedEmbeddingsDimension, embeddings.size());
            assertFalse(embeddings.stream().allMatch(value -> value == 0.0));
        });
    }

    @Test
    void shouldComputeNonZeroEmbeddingsWhenFirstWeightIsZero() {
        int embeddingSize = 128;
        int maxIterations = 4;
        List<Float> weights = List.of(0.0f, 1.0f, 2.0f, 4.0f);
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .algo("gds.alpha.randomProjection")
            .streamMode()
            .addParameter("embeddingSize", embeddingSize)
            .addParameter("iterationWeights", weights)
            .addParameter("maxIterations", maxIterations);

        queryBuilder.addParameter("iterationWeights", weights);
        String query = queryBuilder.yields();

        runQueryWithRowConsumer(query, row -> {
            List<Double> embeddings = (List<Double>) row.get("embedding");
            assertFalse(embeddings.stream().allMatch(value -> value == 0.0));
        });
    }
}
