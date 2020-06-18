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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.AlgoBaseProc;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphalgo.WritePropertyConfigTest;
import org.neo4j.graphalgo.core.CypherMapWrapper;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RandomProjectionWriteProcTest extends RandomProjectionProcTest<RandomProjectionWriteConfig>
    implements WritePropertyConfigTest<RandomProjection, RandomProjectionWriteConfig, RandomProjection> {

    @Override
    public Class<? extends AlgoBaseProc<RandomProjection, RandomProjection, RandomProjectionWriteConfig>> getProcedureClazz() {
        return RandomProjectionWriteProc.class;
    }

    @Override
    public RandomProjectionWriteConfig createConfig(CypherMapWrapper userInput) {
        return RandomProjectionWriteConfig.of(getUsername(), Optional.empty(), Optional.empty(), userInput);
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
    @MethodSource("org.neo4j.gds.embeddings.randomprojections.RandomProjectionsProcTest#weights")
    void shouldComputeNonZeroEmbeddings(List<Float> weights) {
        int embeddingDimension = 128;
        int maxIterations = 4;
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .algo("gds.alpha.randomProjection")
            .writeMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("maxIterations", maxIterations)
            .addParameter("writeProperty", "embedding");

        if (!weights.isEmpty()) {
            queryBuilder.addParameter("iterationWeights", weights);
        }
        String writeQuery = queryBuilder.yields();

        runQuery(writeQuery);

        int expectedEmbeddingsDimension = weights.isEmpty()
            ? embeddingDimension * maxIterations
            : embeddingDimension;
        runQueryWithRowConsumer("MATCH (n:Node) RETURN n.embedding as embedding", row -> {
            double[] embeddings = (double[]) row.get("embedding");
            assertEquals(expectedEmbeddingsDimension, embeddings.length);
            assertFalse(Arrays.stream(embeddings).allMatch(value -> value == 0.0));
        });
    }
}
