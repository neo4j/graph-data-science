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
package org.neo4j.gds.ml.core.functions;

import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.tensor.Matrix;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class L2NormSquaredTest implements FiniteDifferenceTest {

    @Test
    void apply() {
        var weights = new Weights<>(new Matrix(new double[]{
            1, 2, 1,
            3, 2, 1,
            1, 3, 1
        }, 3, 3));

        var l2 = new L2NormSquared(weights);
        var ctx = new ComputationContext();
        var l2Result = ctx.forward(l2);
        assertThat(l2Result.value()).isCloseTo(28, Offset.offset(1e-8));
    }

    @Test
    void shouldApproximateGradient() {
        var weights = new Weights<>(new Matrix(new double[]{
            1, 2, 1,
            3, 2, 1,
            1, 3, 1
        }, 3, 3));

        finiteDifferenceShouldApproximateGradient(weights, new L2NormSquared(weights));
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }
}
