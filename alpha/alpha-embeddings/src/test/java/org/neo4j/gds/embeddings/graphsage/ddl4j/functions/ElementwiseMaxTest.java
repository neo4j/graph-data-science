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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.GraphSageBaseTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.FiniteDifferenceTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.AbstractVariable;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class ElementwiseMaxTest extends GraphSageBaseTest implements FiniteDifferenceTest {

    private Weights weights;

    @BeforeEach
    protected void setup() {
        super.setup();
        weights = new Weights(Tensor.matrix(new double[]{
            1, 2, 3,
            3, 2, 1,
            1, 3, 2
        }, 3, 3));
    }

    @Test
    void testApply() {
        int[][] adjacencyMatrix = {
            new int[]{},
            new int[]{0, 1, 2}
        };
        ElementwiseMax max = new ElementwiseMax(weights, adjacencyMatrix);

        double[] expected = new double[]{
            0, 0, 0,
            3, 3, 3
        };
        assertArrayEquals(expected, ctx.forward(max).data);
    }

    @Test
    void shouldApproximateGradient() {
        int[][] adjacencyMatrix = {
            new int[]{},
            new int[]{0, 1, 2},
            new int[]{}
        };
        Sum sum = new Sum(List.of(new ElementwiseMax(weights, adjacencyMatrix)));
        AbstractVariable loss = new ConstantScale(sum, 2);
        finiteDifferenceShouldApproximateGradient(weights, loss);
    }
}
