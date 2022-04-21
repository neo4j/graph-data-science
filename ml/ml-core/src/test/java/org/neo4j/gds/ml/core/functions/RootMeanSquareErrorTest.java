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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.tensor.Scalar;

import static org.assertj.core.api.Assertions.assertThat;

class RootMeanSquareErrorTest {

    @Test
    void apply() {
        var targets = Constant.vector(new double[]{3, -0.5, 2, 7});
        var predictions = Constant.vector(new double[]{2.5, 0, 2, 8});

        var variable = new RootMeanSquareError(predictions, targets);

        ComputationContext ctx = new ComputationContext();
        assertThat(ctx.forward(variable)).isEqualTo(new Scalar(Math.sqrt(0.375)));
    }

    @ParameterizedTest
    @ValueSource(doubles = {Double.NaN, Double.MAX_VALUE, 1e155})
    void applyOnExtremeValues(double extremeValue) {
        var targets = Constant.vector(new double[]{3, -0.5, 2, 7});
        var predictions = Constant.vector(new double[]{extremeValue, 3, 2, 8});

        var variable = new RootMeanSquareError(predictions, targets);

        ComputationContext ctx = new ComputationContext();
        assertThat(ctx.forward(variable)).isEqualTo(new Scalar(Double.MAX_VALUE));
    }


}
