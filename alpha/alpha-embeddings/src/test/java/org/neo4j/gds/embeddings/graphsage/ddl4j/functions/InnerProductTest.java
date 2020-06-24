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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class InnerProductTest extends GraphSageBaseTest implements FiniteDifferenceTest {

    @Test
    void testApply() {
        Constant vector1 = Constant.vector(new double[] {1, 2, 5});
        Constant vector2 = Constant.vector(new double[] {-1, 0, 5});
        InnerProduct prod = new InnerProduct(vector1, vector2);

        double expected = 24D;

        assertEquals(expected, ctx.forward(prod).data[0]);
    }

    @Test
    void shouldFailOnDimensions() {
        Constant vector1 = Constant.vector(new double[] {1, 2});
        Constant vector2 = Constant.vector(new double[] {-1, 0, 5});
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            new InnerProduct(vector1, vector2);
        });

        assertEquals("Dimensions of left: 2 do not match dimensions of right: 3", ex.getMessage());
    }

    @Test
    void shouldApproximateGradient() {
        Weights vector1 = new Weights(Tensor.vector(new double[] {1, 2, 5}));
        Weights vector2 = new Weights(Tensor.vector(new double[] {6, 2, 5}));
        finiteDifferenceShouldApproximateGradient(
            List.of(vector1, vector2),
            new InnerProduct(vector1, vector2)
        );
    }

}
