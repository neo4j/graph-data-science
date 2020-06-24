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
package org.neo4j.gds.embeddings.graphsage.ddl4j.functions.gds;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.AutogradBaseTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Constant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sum;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class GdsMatrixVectorMultiplyTest extends AutogradBaseTest {

    @Test
    void shouldMultiplyCorrectly() {
        double[] weightsData = {
            1, 2, 3,
            4, 5, 6
        };

        Weights weights = new Weights(new Tensor(weightsData, new int[]{2, 3}));
        Constant vector = Constant.vector(new double[] { 1, 1, 4 });

        GdsMatrixVectorMultiply multiply = new GdsMatrixVectorMultiply(weights, vector);

        assertNotNull(ctx.forward(multiply).data);
        assertArrayEquals(
            new double[]{
                15,
                33
            }, ctx.data(multiply).data);
    }

    @Disabled("The GDS implementation doesn't transpose matrices well")
    @Test
    void shouldComputeGradientCorrectly() {
        double[] weightsData = {
            1, 2, 2,
            0, 1, 4
        };

        double[] vectorData = {2, 1, -2};

        Weights weights = new Weights(new Tensor(weightsData, new int[]{2, 3}));
        Weights vector = new Weights(new Tensor(vectorData, new int[]{3}));

        GdsMatrixVectorMultiply multiply = new GdsMatrixVectorMultiply(weights, vector);


        Sum total = new Sum(List.of(multiply));

        ctx.forward(total);
        ctx.backward(total);

        // multiply.data = [w_00 * v_0 + w_01 * v_1, w_10 * v_0 + w_11 * v_1]
        // total.data = w_00 * v_0 + w_01 * v_1 + w_10 * v_0 + w_11 * v_1

        // multiply.grad == [1, 1]

        // dL/dv_i = dL/dmultiply_0 * dmultiply_0/dv_i + dL/dmultiply_1 * dmultiply_1/dv_i
        // dL/dv_i = multiply.grad[0] * dmultiply_0/dv_i + multiply.grad[1] * dmultiply_1/dv_i
        // dL/dv_i = 1 * dmultiply_0/dv_i + 1 * dmultiply_1/dv_i


        // dL/dv_0 = 1 * dmultiply_0/dv_0 + 1 * dmultiply_1/dv_0 = 1 * w_00 + 1 * w_10
        // dL/dv_1 = 1 * dmultiply_0/dv_1 + 1 * dmultiply_1/dv_1 = 1 * w_01 + 1 * w_11
        // dL/dv_2 = 1 * dmultiply_0/dv_1 + 1 * dmultiply_1/dv_1 = 1 * w_02 + 1 * w_12
        // vector.grad = [w_00 + w_10, w_01 + w_11]  =  ... = weights.T *_mat multiply.grad

        assertArrayEquals(new double[]{1, 1}, ctx.gradient(multiply).data);
        assertArrayEquals(new double[]{1, 3, 6}, ctx.gradient(vector).data);

        //dL/dW_ij = dL/dmultiply_0 * dmultiply_0/dW_ij + dL/dmultiply_1 * dmultiply_1/dW_ij
        //dL/dW_00 = 1 * dmultiply_0/dW_00 + 1 * dmultiply_1/dW_00 = v_0
        //dL/dW_01 = 1 * dmultiply_0/dW_01 + 1 * dmultiply_1/dW_01 = v_1
        //dL/dW_10 = 1 * dmultiply_0/dW_10 + 1 * dmultiply_1/dW_10 = v_0
        //dL/dW_11 = 1 * dmultiply_0/dW_11 + 1 * dmultiply_1/dW_11 = v_1

        assertArrayEquals(new double[]{2, 1, -2, 2, 1, -2}, ctx.gradient(weights).data);
        // [1, 1] *outerproduct [2, 1, -2]

        // mult = W * v
        // d mult/ dW = v
        // d mult/ dv = W
    }

    @Test
    void shouldThrowWhenDimensionsMismatch() {
        Weights weights = new Weights(new Tensor(new double[]{1, 2, 3, 5, 5, 6}, new int[]{2, 3}));
        Constant vector = Constant.vector(new double[] { 1, 1 });

        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new GdsMatrixVectorMultiply(weights, vector)
        );

        assertEquals("Cannot multiply matrix having 3 columns with a vector of size 2", exception.getMessage());
    }

}
