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

class MatrixVectorSumTest extends GraphSageBaseTest implements FiniteDifferenceTest {

    @Test
    void shouldBroadcastSum() {
        Constant matrix = Constant.matrix(new double[]{1, 2, 3, 4, 5, 7}, 2, 3);
        Constant vector = Constant.vector(new double[]{1, 1, 1});

        Variable broadcastSum = new MatrixVectorSum(matrix, vector);
        double[] result = ctx.forward(broadcastSum).data;

        assertArrayEquals(new double[] {2, 3, 4, 5, 6, 8}, result);
    }

    @Test
    void shouldApproximateGradient() {
        Weights weights = new Weights(Tensor.matrix(new double[]{1, 2, 3, 4, 5, 7}, 2, 3));
        Weights vector = new Weights(Tensor.vector(new double[]{1, 1, 1}));

        Variable broadcastSum = new Sum(List.of(new MatrixVectorSum(weights, vector)));

        finiteDifferenceShouldApproximateGradient(List.of(weights, vector), broadcastSum);
    }

}
