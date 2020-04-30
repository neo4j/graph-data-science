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
package org.neo4j.graphalgo.embedding;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.graphalgo.BaseProcTest;
import org.neo4j.graphalgo.GdsCypher;
import org.neo4j.graphalgo.Orientation;
import org.neo4j.graphdb.QueryExecutionException;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RandomProjectionStreamProcTest extends BaseProcTest {

    private static final String DB_CYPHER = "CREATE" +
                                            "  (a:Node)" +
                                            ", (b:Node)" +
                                            ", (c:Isolated)" +
                                            ", (d:Isolated)" +
                                            ", (a)-[:REL]->(b)";

    @BeforeEach
    void setupGraphDb() throws Exception {
        runQuery(DB_CYPHER);
        registerProcedures(RandomProjectionStreamProc.class);
    }

    @ParameterizedTest
    @MethodSource("weights")
    void shouldComputeNonZeroEmbeddings(List<Float> weights) {
        int embeddingDimension = 128;
        int maxIterations = 4;
        GdsCypher.ParametersBuildStage queryBuilder = GdsCypher.call()
            .withNodeLabel("Node")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .algo("gds.alpha.randomProjection")
            .streamMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("maxIterations", maxIterations);

        if (!weights.isEmpty()) {
            queryBuilder.addParameter("iterationWeights", weights);
        }
        String query = queryBuilder.yields();

        int expectedEmbeddingsDimension = weights.isEmpty()
            ? embeddingDimension * maxIterations
            : embeddingDimension;
        runQueryWithRowConsumer(query, row -> {
            List<Double> embeddings = (List<Double>) row.get("embeddings");
            assertEquals(expectedEmbeddingsDimension, embeddings.size());
            assertFalse(embeddings.stream().allMatch(value -> value == 0.0));
        });
    }

    @Test
    void shouldYieldEmptyEmbeddingForIsolatedNodes() {
        int embeddingDimension = 128;
        int maxIterations = 4;
        String query = GdsCypher.call()
            .withNodeLabel("Isolated")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .algo("gds.alpha.randomProjection")
            .streamMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("maxIterations", maxIterations)
            .yields();

        runQueryWithRowConsumer(query, row -> {
            List<Double> embeddings = (List<Double>) row.get("embeddings");
            assertEquals(embeddingDimension * maxIterations, embeddings.size());
            assertTrue(embeddings.stream().allMatch(value -> value == 0.0));
        });
    }

    @Test
    void shouldFailWhenWeightsLengthUnequalToIterations() {
        int embeddingDimension = 128;
        int maxIterations = 4;
        String query = GdsCypher.call()
            .withNodeLabel("Isolated")
            .withRelationshipType("REL", Orientation.UNDIRECTED)
            .algo("gds.alpha.randomProjection")
            .streamMode()
            .addParameter("embeddingDimension", embeddingDimension)
            .addParameter("maxIterations", maxIterations)
            .addParameter("iterationWeights", List.of(1.0f, 1.0f))
            .yields();

        QueryExecutionException ex = assertThrows(QueryExecutionException.class, () -> {
            runQuery(query);
        });

        assertThat(ex.getMessage(), containsString("Parameter `iterationWeights` should have `maxIterations` entries"));
    }

    private static Stream<Arguments> weights() {
        return Stream.of(
            Arguments.of(Collections.emptyList()),
            Arguments.of(List.of(1.0f, 1.0f, 2.0f, 4.0f ))
        );
    }
}