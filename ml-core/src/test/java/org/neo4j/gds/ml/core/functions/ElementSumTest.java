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
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.FiniteDifferenceTest;
import org.neo4j.gds.ml.core.helper.Constant;
import org.neo4j.gds.ml.core.tensor.Scalar;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ElementSumTest implements FiniteDifferenceTest {

    @Test
    void adds() {
        var operand = new Constant<>(new Scalar(1.4));
        var add = new ElementSum(List.of(operand, operand, operand));

        assertThat(add.dimensions()).containsExactly(Dimensions.scalar());

        ComputationContext ctx = new ComputationContext();
        var forward = ctx.forward(add);
        assertThat(forward.value()).isCloseTo(4.2, Offset.offset(1e-8));
        assertThat(forward.dimensions()).containsExactly(Dimensions.scalar());
    }

    @Test
    void shouldApproximateGradient() {

        var operand = new Weights<>(new Scalar(1.4));
        var add = new ElementSum(List.of(operand, operand, operand));

        finiteDifferenceShouldApproximateGradient(operand, add);
    }

}
