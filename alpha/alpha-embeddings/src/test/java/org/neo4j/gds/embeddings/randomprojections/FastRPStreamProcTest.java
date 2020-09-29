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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@SuppressWarnings("unchecked")
class FastRPStreamProcTest extends FastRPProcTest<FastRPStreamConfig> {

    @Override
    public Class<? extends AlgoBaseProc<FastRP, FastRP, FastRPStreamConfig>> getProcedureClazz() {
        return RandomProjectionStreamProc.class;
    }

    @Override
    public FastRPStreamConfig createConfig(CypherMapWrapper userInput) {
        return FastRPStreamConfig.of(getUsername(), Optional.empty(), Optional.empty(), userInput);
    }

    @ParameterizedTest
    @MethodSource("org.neo4j.gds.embeddings.randomprojections.FastRPProcTest#weights")
    void shouldComputeNonZeroEmbeddings(List<Float> weights) {
        int embeddingSize = 128;
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .algo("gds.alpha.randomProjection")
            .streamMode()
            .addParameter("embeddingSize", embeddingSize);

        if (!weights.isEmpty()) {
            queryBuilder.addParameter("iterationWeights", weights);
        }
        String query = queryBuilder.yields();

        runQueryWithRowConsumer(query, row -> {
            List<Float> embeddings = (List<Float>) row.get("embedding");
            assertEquals(embeddingSize, embeddings.size());
            assertFalse(embeddings.stream().allMatch(value -> value == 0.0));
        });
    }

    @Test
    void shouldComputeNonZeroEmbeddingsWhenFirstWeightIsZero() {
        int embeddingSize = 128;
        List<Float> weights = List.of(0.0f, 1.0f, 2.0f, 4.0f);
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .algo("gds.alpha.randomProjection")
            .streamMode()
            .addParameter("embeddingSize", embeddingSize)
            .addParameter("iterationWeights", weights);

        queryBuilder.addParameter("iterationWeights", weights);
        String query = queryBuilder.yields();

        runQueryWithRowConsumer(query, row -> {
            List<Float> embeddings = (List<Float>) row.get("embedding");
            assertFalse(embeddings.stream().allMatch(value -> value == 0.0));
        });
    }

    @Test
    void shouldComputeWithWeight() {
        int embeddingSize = 128;
        String query = GdsCypher.call()
            .withNodeLabel("Node")
            .withNodeLabel("Node2")
            .withRelationshipType("REL2")
            .withRelationshipProperty("weight")
            .algo("gds.alpha.randomProjection")
            .streamMode()
            .addParameter("embeddingSize", embeddingSize)
            .addParameter("relationshipWeightProperty", "weight")
            .yields();

        List<List<Float>> embeddings = new ArrayList<>(3);
        runQueryWithRowConsumer(query, row -> {
            embeddings.add((List<Float>) row.get("embedding"));
        });

        for (int i = 0; i < 128; i++) {
            assertEquals(embeddings.get(1).get(i), embeddings.get(2).get(i) * 2);
        }
    }
}
