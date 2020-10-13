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
package org.neo4j.gds.embeddings.graphsage.weighted;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.FiniteDifferenceTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.GraphSageBaseTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class HingeLossTest extends GraphSageBaseTest implements FiniteDifferenceTest {

    @Test
    void shouldComputeLossBatchSizeOne() {
        Variable<Matrix> combinedEmbeddings = new MatrixConstant(
            new double[]{
                1.5, -1, 0.75,  // nodeId
                1, -0.75, 0.7,  // positive nodeId
                -0.1, 0.4, 0.1  // negative nodeId
            },
            3, 3
        );

        var positive = (1.5 * 1) + (-1 * -0.75) + (0.75 * 0.7);
        var negative = (1.5 * -0.1) + (-1 * 0.4) + (0.75 * 0.1);

        double expected = Math.max(0, (-positive + negative));

        Variable<Scalar> lossVar = new HingeLoss(combinedEmbeddings);

        Tensor<?> lossData = ctx.forward(lossVar);
        assertNotNull(lossData);

        assertEquals(expected, lossData.dataAt(0));
    }

    @Test
    void shouldComputeLoss() {
        Variable<Matrix> combinedEmbeddings = new MatrixConstant(
            new double[]{
                1.5, -1, 0.75,      // nodeId
                0.5, -0.1, 0.7,     // nodeId
                0.23, 0.001, 0.3,   // nodeId
                1, -0.75, 0.7,      // positive nodeId
                0.1, -0.125, 0.45,  // positive nodeId
                0.2, 0.01, 0.24,    // positive nodeId
                -0.1, 0.4, 0.1,     // negative nodeId
                -0.9, 0.7, -0.3,    // negative nodeId
                -0.25, 0.4, -0.2    // negative nodeId
            },
            9, 3
        );

        var n1P = (1.5 * 1) + (-1 * -0.75) + (0.75 * 0.7);
        var n1N = (1.5 * -0.1) + (-1 * 0.4) + (0.75 * 0.1);

        var n2P = (0.5 * 0.1) + (-0.1 * -0.125) + (0.7 * 0.45);
        var n2N = (0.5 * -0.9) + (-0.1 * 0.7) + (0.7 * -0.3);

        var n3P = (0.23 * 0.2) + (0.001 * 0.01) + (0.3 * 0.24);
        var n3N = (0.23 * -0.25) + (0.001 * 0.4) + (0.3 * -0.2);

        var expected = Math.max(
            0,
            (-n1P + n1N) +
            (-n2P + n2N) +
            (-n3P + n3N)
        );

        Variable<Scalar> lossVar = new HingeLoss(combinedEmbeddings);

        Tensor<?> lossData = ctx.forward(lossVar);
        assertNotNull(lossData);
        assertEquals(expected, lossData.dataAt(0), 1e-10);
    }

    @Test
    void testGradient() {
        Weights<Matrix> combinedEmbeddings = new Weights<>(new Matrix(
            new double[]{
                1.5, -1, 0.75,  // nodeId
                1, -0.75, 0.7,  // positive nodeId
                -0.1, 0.4, 0.1  // negative nodeId
            },
            3, 3
        ));

        finiteDifferenceShouldApproximateGradient(combinedEmbeddings, new HingeLoss(combinedEmbeddings));

    }

    @Override
    public double epsilon() {
        return 1e-6;
    }
}
