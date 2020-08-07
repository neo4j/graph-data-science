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
package org.neo4j.gds.embeddings.graphsage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.neo4j.gds.embeddings.graphsage.ddl4j.FiniteDifferenceTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.GraphSageBaseTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GraphSageLossTest extends GraphSageBaseTest implements FiniteDifferenceTest {

    @ParameterizedTest
    @CsvSource({
        "1, 0.5440720306533048",
        "4, 1.9948392942657789",
        "8, 3.9291956457490773",
        "12, 5.863551997232376",
        "20, 9.732264700198973",
        "50, 24.239937336323717"
    })
    void shouldComputeLossBatchSizeOne(int Q, double expectedLoss) {
        Variable<Matrix> combinedEmbeddings = new MatrixConstant(
            new double[]{
                1.5, -1, 0.75,  // nodeId
                1, -0.75, 0.7,  // positive nodeId
                -0.1, 0.4, 0.1  // negative nodeId
            },
            3, 3
        );

        Variable<Scalar> lossVar = new GraphSageLoss(combinedEmbeddings, Q);

        Tensor lossData = ctx.forward(lossVar);
        assertNotNull(lossData);

        assertEquals(expectedLoss, lossData.getAtIndex(0));
    }

    @ParameterizedTest
    @CsvSource({
        "1, 2.7317010501515524",
        "4, 7.2713924182831615",
        "8, 13.324314242458641",
        "12, 19.37723606663412",
        "20, 31.48307971498508",
        "50, 76.87999339630119"
    })
    void shouldComputeLoss(int Q, double expectedLoss) {
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

        Variable<Scalar> lossVar = new GraphSageLoss(combinedEmbeddings, Q);

        Tensor lossData = ctx.forward(lossVar);
        assertNotNull(lossData);
        assertEquals(expectedLoss, lossData.getAtIndex(0), 1e-10);
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

        finiteDifferenceShouldApproximateGradient(combinedEmbeddings, new GraphSageLoss(combinedEmbeddings, 5));

    }

    @Override
    public double epsilon() {
        return 1e-6;
    }
}
