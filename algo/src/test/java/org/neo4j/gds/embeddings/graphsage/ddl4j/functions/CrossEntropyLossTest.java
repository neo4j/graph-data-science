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

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.FiniteDifferenceTest;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;

import static java.lang.Math.log;
import static org.assertj.core.api.Assertions.assertThat;

class CrossEntropyLossTest implements FiniteDifferenceTest {

    @Test
    void shouldApplyCorrectly() {
        MatrixConstant targets = new MatrixConstant(new double[]{1.0, 0.0, 1.0}, 3, 1);
        MatrixConstant predictions = new MatrixConstant(new double[]{0.35, 0.41, 1.0}, 3, 1);
        CrossEntropyLoss loss = new CrossEntropyLoss(predictions, targets);
        ComputationContext ctx = new ComputationContext();
        double lossValue = ctx.forward(loss).value();
        // -1/3 sum 1.0*log(0.34) + 0 + 0 + log(1-0.41) + log(1.0) + 0
        double expectedValue = -1.0/3.0 * (1.0*log(0.35) + 0 + 0 + log(1-0.41) + log(1.0) + 0);
        assertThat(lossValue).isCloseTo(expectedValue, Offset.offset(1e-8));
    }

    @Test
    void shouldApproximateGradient() {
        MatrixConstant targets = new MatrixConstant(new double[]{1.0, 0.0, 1.0}, 3, 1);
        Weights<Matrix> predictions = new Weights<>(new Matrix(new double[]{0.35, 0.41, 1.0}, 3, 1));
        CrossEntropyLoss loss = new CrossEntropyLoss(predictions, targets);
        finiteDifferenceShouldApproximateGradient(predictions, loss);
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }
}
