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
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class MultiMeanTest extends ComputationContextBaseTest implements FiniteDifferenceTest {
    @Test
    void shouldAverage() {
        // a    a
        // b    b
        // c
        // d
        // (a)--(b), (c), (b)--(d)

        double[] matrix = {
            1, 2,
            4, 5,
            5, 2,
            6, -1
        };

        int[][] adj = new int[2][];
        adj[0] = new int[] {1};
        adj[1] = new int[] {0, 3};
        int[] selfAdjacency = {0, 1};

        var expected = new Matrix(new double[]{
            2.5, 3.5,
            11.0 / 3, 2
        }, 2, 2);

        var data = Constant.matrix(matrix, 4, 2);
        var mean = new MultiMean(data, adj, selfAdjacency);

        assertThat(ctx.forward(mean)).isEqualTo(expected);
    }


    @Test
    void testGradient() {
        double[] matrix = {
            1, 2,
            4, 5,
            3, 6
        };

        int[][] adj = new int[2][];
        adj[0] = new int[] {1};
        adj[1] = new int[] {0, 2};
        int[] selfAdjacency = {0, 1};

        Weights<Matrix> weights = new Weights<>(new Matrix(matrix, 3, 2));

        finiteDifferenceShouldApproximateGradient(weights, new ElementSum(List.of(new MultiMean(weights, adj, selfAdjacency))));
    }

}
