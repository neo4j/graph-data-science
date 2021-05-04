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
package org.neo4j.gds.core.ml.functions;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.core.ml.ComputationContextBaseTest;
import org.neo4j.gds.core.ml.FiniteDifferenceTest;
import org.neo4j.gds.core.ml.Variable;
import org.neo4j.gds.core.ml.tensor.Matrix;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

class ConstantScaleTest extends ComputationContextBaseTest implements FiniteDifferenceTest {
    @Test
    void testApply() {
        Weights<Matrix> matrix = new Weights<>(new Matrix(new double[]{1, 2, 3, 4}, 2, 2));
        double constant = 5.34D;
        Variable<Matrix> scaled = new ConstantScale<>(matrix, constant);

        assertArrayEquals(new double[]{constant, 2 * constant, 3 * constant, 4 * constant}, ctx.forward(scaled).data());
    }


    @Test
    void shouldApproximateGradient() {
        Weights<Matrix> matrix = new Weights<>(new Matrix(new double[]{1, 2, 3, 4}, 2, 2));
        double constant = 5.34D;
        finiteDifferenceShouldApproximateGradient(
            matrix,
            new ElementSum(List.of(new ConstantScale<>(matrix, constant)))
        );
    }

}
