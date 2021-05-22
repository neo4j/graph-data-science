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
package org.neo4j.gds.ml.core.functions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContextBaseTest;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ElementWiseMaxTest extends ComputationContextBaseTest implements FiniteDifferenceTest {

    @Test
    void testApply() {
        var parent = new Weights<>(new Matrix(new double[]{
            1, 2, 3,
            5, 2, 1,
            9, 4, 2,
            1, 1, 1
        }, 4, 3));

        var adjacencyMatrix = new int[2][3];

        // Node 0 --> no neighbours
        adjacencyMatrix[0] = new int[]{};

        // Node 1 --> three neighbours
        adjacencyMatrix[1] = new int[]{0, 1, 2};

        Variable<Matrix> max = new ElementWiseMax(parent, adjacencyMatrix);

        var expected = new Matrix(new double[]{
            0, 0, 0,    // Node 0 --> no neighbours --> 0s
            9, 4, 3     //
        }, 2, 3);

        assertThat(ctx.forward(max)).isEqualTo(expected);
    }

    @Test
    void shouldApproximateGradient() {
        var weights = new Weights<>(new Matrix(new double[]{
            1, 2, 3,
            3, 2, 1,
            1, 3, 2
        }, 3, 3));

        int[][] adjacencyMatrix = {
            new int[]{},
            new int[]{0, 1, 2},
            new int[]{}
        };
        ElementSum sum = new ElementSum(List.of(new ElementWiseMax(weights, adjacencyMatrix)));
        Variable<Scalar> loss = new ConstantScale<>(sum, 2);
        finiteDifferenceShouldApproximateGradient(weights, loss);
    }
}
