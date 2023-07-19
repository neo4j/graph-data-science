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
package org.neo4j.gds.similarity.nodesim;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class CosineSimilarityComputerTest {

    @Test
    void shouldComputeUnweightedCosineSimilarity() {
        var array1 = new long[]{0, 1, 2, 4, 6};
        var array2 = new long[]{0, 1, 2, 3, 5, 6, 7};
        var similarityComputer = new CosineSimilarityComputer(0);

        var cosineSimilarity = similarityComputer.computeSimilarity(array1, array2);

        assertThat(cosineSimilarity).isCloseTo(0.6761234037828131, Offset.offset(1e-5));
    }

    @Test
    void shouldComputeUnweightedCosineSimilarityRespectingCutoff() {
        var array1 = new long[]{0, 1, 2, 4, 6};
        var array2 = new long[]{0, 1, 2, 3, 5, 6, 7};
        var similarityComputer = new CosineSimilarityComputer(0.7);

        var cosineSimilarity = similarityComputer.computeSimilarity(array1, array2);

        assertThat(cosineSimilarity).isNaN();
    }

    @Test
    void shouldComputeCosineOnArraysWithNoMissingElements() {
        var similarityComputer = new CosineSimilarityComputer(0);
        var array1 = new long[]{0, 1, 2};
        var weight1 = new double[]{0.5, 0.2, 0.6};
        var array2 = new long[]{0, 1, 2};
        var weight2 = new double[]{1.3, 0.9, 0.2};

        assertThat(similarityComputer.computeWeightedSimilarity(array1, array2, weight1, weight2)).isCloseTo(
            0.73935,
            Offset.offset(1e-5)
        );
    }

    @Test
    void shouldComputeCosineOnArraysWithNoMissingElementsRespectingCutoff() {
        var similarityComputer = new CosineSimilarityComputer(1);
        var array1 = new long[]{0, 1, 2};
        var weight1 = new double[]{0.5, 0.2, 0.6};
        var array2 = new long[]{0, 1, 2};
        var weight2 = new double[]{1.3, 0.9, 0.2};

        assertThat(similarityComputer.computeWeightedSimilarity(array1, array2, weight1, weight2)).isNaN();
    }

    @Test
    void shouldComputeCosineOnArraysWithMissingElements() {
        var similarityComputer = new CosineSimilarityComputer(0);
        var array1 = new long[]{0, 1, 2, 4, 6};
        var weight1 = new double[]{0.5, 0.2, 0.6, 0.8, 1};
        var array2 = new long[]{0, 1, 2, 3, 5, 6, 7};
        var weight2 = new double[]{1.3, 0.9, 0.2, 4.2, 1, 2, 3};

        assertThat(similarityComputer.computeWeightedSimilarity(array1, array2, weight1, weight2)).isCloseTo(
            0.33344,
            Offset.offset(1e-5)
        );
    }

}
