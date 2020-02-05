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
package org.neo4j.graphalgo.impl.similarity;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.graphalgo.TestDatabaseCreator;
import org.neo4j.graphalgo.core.ProcedureConstants;
import org.neo4j.graphalgo.newapi.GraphCreateConfig;
import org.neo4j.graphalgo.results.SimilarityResult;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;

class WeightedSimilarityAlgorithmTest {
    private GraphDatabaseAPI db;

    @BeforeEach
    void setupGraph() {
        db = TestDatabaseCreator.createTestDatabase();
    }

    @Test
    void prepareInputsShouldThrowWhenColumnMissing() {
        String badQuery = "UNWIND [[2.5, 1, 81],[100, 42, 162]] AS x RETURN x[0] AS weight, x[1] AS dogegory, x[2] AS item";
        assertThrows(IllegalArgumentException.class, () -> runPrepareWeightsOnQuery(badQuery));
    }

    @Test
    void prepareInputsOkOnGoodQuery() {
        String goodQuery = "UNWIND [[2.5, 1, 81],[100, 42, 162]] AS x RETURN x[0] AS weight, x[1] AS category, x[2] AS item";
        runPrepareWeightsOnQuery(goodQuery);
    }

    private void runPrepareWeightsOnQuery(String query) {
        SimilarityConfig similarityConfig = getDummyConfig();

        DummySimilarityAlgorithm dummySimilarityAlgorithm = new DummySimilarityAlgorithm(similarityConfig, db);
        dummySimilarityAlgorithm.prepareInputs(query, similarityConfig);
    }

    private static class DummySimilarityAlgorithm extends WeightedSimilarityAlgorithm<DummySimilarityAlgorithm> {
        DummySimilarityAlgorithm(SimilarityConfig config, GraphDatabaseAPI api) {
            super(config, api);
        }

        @Override
        SimilarityComputer<WeightedInput> similarityComputer(
            Double skipValue,
            int[] sourceIndexIds,
            int[] targetIndexIds
        ) {
            return (decoder, s, t, cutoff) -> new SimilarityResult(0, 0, 0, 0, 0, 0);
        }

        @Override
        public void assertRunning() {
        }
    }

    private SimilarityConfig getDummyConfig() {
        return new SimilarityConfig() {
            @Override
            public Optional<String> graphName() {
                return Optional.empty();
            }

            @Override
            public String graph() {
                return ProcedureConstants.CYPHER_QUERY_KEY;
            }

            @Override
            public Optional<GraphCreateConfig> implicitCreateConfig() {
                return Optional.empty();
            }
        };
    }
}