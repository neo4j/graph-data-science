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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.GraphSageBaseTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.FiniteDifferenceTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class MatrixMultiplyWithTransposedSecondOperandTest extends GraphSageBaseTest implements FiniteDifferenceTest {

    @Override
    public double epsilon() {
        return 1e-6;
    }

    @Test
    void testMultiply() {
        double[] m1 = {
            1, 2, 3,
            4, 5, 6
        };

        double[] m2 = {
            1, 4, 6,
            2.1, 5, -1
        };

        double[] expected = {
            27,  9.1,
            60, 27.4
        };

        Constant A = Constant.matrix(m1, 2, 3);
        Constant B = Constant.matrix(m2, 2, 3);

        Variable product = new MatrixMultiplyWithTransposedSecondOperand(A, B);
        double[] result = ctx.forward(product).data;

        assertArrayEquals(expected, result);
    }

    @Test
    void shouldApproximateGradient() {
        double[] m1 = {
            1, 2, 3,
            4, 5, 6
        };

        double[] m2 = {
            1, 4, 6,
            2.1, 5, -1
        };

        Weights A = new Weights(Tensor.matrix(m1, 2, 3));
        Weights B = new Weights(Tensor.matrix(m2, 2, 3));

        finiteDifferenceShouldApproximateGradient(List.of(A, B), new L2Norm(new MatrixMultiplyWithTransposedSecondOperand(A, B)));
    }

}
