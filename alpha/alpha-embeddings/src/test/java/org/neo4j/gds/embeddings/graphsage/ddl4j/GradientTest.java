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
package org.neo4j.gds.embeddings.graphsage.ddl4j;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.TensorAdd;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.helper.Constant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.helper.Sum;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Vector;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class GradientTest {

    @Test
    void matrixAddition() {
        var operand1 = new Weights<>(Matrix.fill(5, 5, 1));
        var operand2 = new Constant<>(Matrix.fill(4, 5, 1));
        var add = new TensorAdd(List.of(operand1, operand2));
        var sum = new Sum(List.of(add));

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
        assertEquals(45D, ctx.data(sum).getAtIndex(0));
        assertArrayEquals(new int[]{5, 1}, ctx.data(add).dimensions());

        assertArrayEquals(Vector.fill(9D, 5).data(), ctx.data(add).data());

        ctx.backward(sum);

        assertNull(ctx.gradient(operand2), "Gradient should be null for Constant");
        assertArrayEquals(new int[]{5, 1}, ctx.gradient(add).dimensions());
        assertArrayEquals(new int[]{5, 1}, ctx.gradient(operand1).dimensions());

        assertArrayEquals(Vector.fill(1D, 5).data(), ctx.gradient(add).data());
        assertArrayEquals(Vector.fill(1D, 5).data(), ctx.gradient(operand1).data());
    }
}
