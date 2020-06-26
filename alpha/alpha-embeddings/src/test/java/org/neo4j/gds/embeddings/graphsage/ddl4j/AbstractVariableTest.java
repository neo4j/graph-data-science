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
import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Constant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sum;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.TensorAdd;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AbstractVariableTest {
    @Test
    void testAdd() {

        ComputationContext ctx = new ComputationContext();
        int[] dimensions = {5};
        var x = new Weights(Tensor.constant(5, dimensions));
        var y = new Constant(Tensor.constant(4, dimensions));
        var z = new TensorAdd(List.of(x, y), dimensions);
        var w = new Sum(List.of(z));

        assertNull(ctx.gradient(x), "Gradient should be null before forward");
        assertNull(ctx.gradient(y), "Gradient should be null before forward");
        assertNull(ctx.gradient(w), "Gradient should be null before forward");
        assertNull(ctx.gradient(z), "Gradient should be null before forward");
        ctx.forward(w);

        assertNull(ctx.gradient(x), "Gradient should be null after forward");
        assertNull(ctx.gradient(y), "Gradient should be null after forward");
        assertNull(ctx.gradient(w), "Gradient should be null after forward");
        assertNull(ctx.gradient(z), "Gradient should be null after forward");
        assertArrayEquals(new int[]{1}, w.dimensions());
        assertEquals(45D, ctx.data(w).getAtIndex(0));
        assertArrayEquals(new int[]{5}, ctx.data(z).dimensions);
        for (int i = 0; i < 5; i++) {
            assertEquals(9D, ctx.data(z).getAtIndex(i));
        }

        ctx.backward(w);

        assertNull(ctx.gradient(y), "Gradient should be null for Constant");
        assertArrayEquals(new int[]{5}, ctx.gradient(z).dimensions);
        assertArrayEquals(new int[]{5}, ctx.gradient(x).dimensions);
        for (int i = 0; i < 5; i++) {
            assertEquals(1D, ctx.gradient(z).getAtIndex(i));
            assertEquals(1D, ctx.gradient(x).getAtIndex(i));
        }

    }
}
