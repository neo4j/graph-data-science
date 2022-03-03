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

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.tensor.Vector;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MeanSquaredErrorTest implements FiniteDifferenceTest {
    @Test
    void testForward() {
        ComputationContext ctx = new ComputationContext();
        var a = new Weights<>(new Vector(new double[]{1, 1, 1, 1, 1, 1}));
        var b = new Weights<>(new Vector(new double[]{3, 2, 2, 2, 2, 1}));

        var meanSquaredError = new MeanSquaredError(a, b);
        assertThat(ctx.forward(meanSquaredError).value()).isEqualTo((4 + 1 + 1 + 1 + 1 + 0) / 6.0);
    }

    @Test
    void testGradient() {
        var a = new Weights<>(new Vector(new double[]{1, 1, 1, 1, 1, 1}));
        var b = new Weights<>(new Vector(new double[]{3, 2, 2, 2, 2, 1}));

        finiteDifferenceShouldApproximateGradient(List.of(a, b), new MeanSquaredError(a, b));
    }

    @Test
    void failOnWrongSizedArguments() {
        assertThatThrownBy(() -> new MeanSquaredError(Constant.vector(new double[]{1, 1, 1, 1, 1, 1}), Constant.vector(new double[]{1, 1, 1, 1, 1})))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Targets and predictions must be of equal size. Got predictions: Vector(6), targets: Vector(5)");
    }

    @Override
    public double epsilon() {
        return 1e-7;
    }
}
