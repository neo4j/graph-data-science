/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.FiniteDifferenceTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.GraphSageBaseTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.helper.ElementSum;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class SliceTest extends GraphSageBaseTest implements FiniteDifferenceTest {

    @Test
    void shouldSlice() {

        Weights<Matrix> weights = new Weights<>(new Matrix(new double[]{
            1, 2, 3,
            3, 2, 1,
            1, 3, 2
        }, 3, 3));

        int[] rows = new int[] {0, 2, 0};
        Variable<Matrix> slice = new Slice(weights, rows);
        double[] result = ctx.forward(slice).data();

        assertArrayEquals(
            new double[]{
                1, 2, 3,
                1, 3, 2,
                1, 2, 3
            }, result);
    }

    @Test
    void shouldApproximateGradient() {
        Weights<Matrix> weights = new Weights<>(new Matrix(new double[]{
            1, 2, 3,
            3, 2, 1,
            1, 3, 2
        }, 3, 3));

        int[] rows = new int[] {0, 2, 0};
        Variable<Scalar> sum = new ElementSum(List.of(new Slice(weights, rows)));

        finiteDifferenceShouldApproximateGradient(weights, sum);
    }
}
