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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

class JaccardSimilarityComputerTest {

    @Test
    void shouldComputeCorrectSimilarityWhenInputVectorsHaveDifferentSizes() {
        var vector1 = new long[]{1, 5, 4, 6, 3};
        var vector2 = new long[]{1, 3, 4, 5};
        var weights1 = new double[]{
            0.5801196133134187,
            0.5801196133134187,
            0.5551196133134186,
            0.8213475204444817,
            0.5051196133134187
        };
        var weights2 = new double[]{0.5801196133134187, 0.5051196133134187, 0.5551196133134186, 0.5801196133134187};

        var computer = new JaccardSimilarityComputer(1.0E-42);

        var similarity = computer.computeWeightedSimilarity(
            vector1,
            vector2,
            weights1,
            weights2
        );

        assertThat(similarity).isCloseTo(0.7299820806494354, Offset.offset(1e-15));
    }

    @Test
    void shouldComputeCorrectSimilarityWhenInputVectorsHaveSameSizes() {
        var vector1 = new long[]{1, 5, 4, 6, 3};
        var vector2 = new long[]{1, 3, 4, 5, 6};
        var weights1 = new double[]{
            0.5801196133134187,
            0.5801196133134187,
            0.5551196133134186,
            0.8213475204444817,
            0.5051196133134187
        };
        var weights2 = new double[]{
            0.5801196133134187,
            0.5051196133134187,
            0.5551196133134186,
            0.5801196133134187,
            0.0
        };

        var computer = new JaccardSimilarityComputer(1.0E-42);

        var similarity = computer.computeWeightedSimilarity(
            vector1,
            vector2,
            weights1,
            weights2
        );

        assertThat(similarity).isCloseTo(0.7299820806494354, Offset.offset(1e-15));
    }

}
