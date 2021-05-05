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
package org.neo4j.gds.ml.core;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.MatrixSum;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.helper.Constant;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.graphalgo.core.utils.ArrayUtil;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class GradientTest {

    @Test
    void matrixAddition() {
        var operand1 = new Weights<>(Matrix.fill(5, 5, 1));
        var operand2 = new Constant<>(Matrix.fill(4, 5, 1));
        var add = new MatrixSum(List.of(operand1, operand2));
        var sum = new ElementSum(List.of(add));

        ComputationContext ctx = new ComputationContext();
        assertNull(ctx.gradient(operand1), "Gradient should be null before forward");
        assertNull(ctx.gradient(operand2), "Gradient should be null before forward");
        assertNull(ctx.gradient(sum), "Gradient should be null before forward");
        assertNull(ctx.gradient(add), "Gradient should be null before forward");

        ctx.forward(sum);

        assertNull(ctx.gradient(operand1), "Gradient should be null after forward");
        assertNull(ctx.gradient(operand2), "Gradient should be null after forward");
        assertNull(ctx.gradient(sum), "Gradient should be null after forward");
        assertNull(ctx.gradient(add), "Gradient should be null after forward");

        assertArrayEquals(Dimensions.scalar(), sum.dimensions());
        assertEquals(45D, ctx.data(sum).value());
        assertArrayEquals(new int[]{5, 1}, ctx.data(add).dimensions());

        assertArrayEquals(ArrayUtil.fill(9D, 5), ctx.data(add).data());

        ctx.backward(sum);

        assertNull(ctx.gradient(operand2), "Gradient should be null for Constant");
        assertArrayEquals(new int[]{5, 1}, ctx.gradient(add).dimensions());
        assertArrayEquals(new int[]{5, 1}, ctx.gradient(operand1).dimensions());

        assertArrayEquals(ArrayUtil.fill(1D, 5), ctx.gradient(add).data());
        assertArrayEquals(ArrayUtil.fill(1D, 5), ctx.gradient(operand1).data());
    }
}
