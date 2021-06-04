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
package org.neo4j.gds.ml.core;

import org.junit.jupiter.api.Test;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.ConstantScale;
import org.neo4j.gds.ml.core.functions.ElementSum;
import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Scalar;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class ComputationContextTest {

    @Test
    void render() {
        var ctx = new ComputationContext();

        var a = Constant.scalar(2);
        var b = new ConstantScale<>(a, 3);
        var c = new Weights<>(new Scalar(4));
        var d = new ElementSum(List.of(a, b, c));

        ctx.forward(d);

        assertThat(ctx.render()).isEqualTo(
            "ConstantScale: scale by 3.0, requireGradient: false" + System.lineSeparator() +
            " \t data: Scalar: [6.0]" + System.lineSeparator() +
            "Weights: Scalar: [4.0], requireGradient: true" + System.lineSeparator() +
            " \t data: Scalar: [4.0]" + System.lineSeparator() +
            "Constant: Scalar: [2.0], requireGradient: false" + System.lineSeparator() +
            " \t data: Scalar: [2.0]" + System.lineSeparator() +
            "ElementSum: Vector(1), requireGradient: true" + System.lineSeparator() +
            " \t data: Scalar: [12.0]" + System.lineSeparator());

        ctx.backward(d);

        assertThat(ctx.render()).isEqualTo(
            "ConstantScale: scale by 3.0, requireGradient: false" + System.lineSeparator() +
            " \t data: Scalar: [6.0]" + System.lineSeparator() +
            "Weights: Scalar: [4.0], requireGradient: true" + System.lineSeparator() +
            " \t data: Scalar: [4.0]" + System.lineSeparator() +
            "\t gradient: Scalar: [1.0]" + System.lineSeparator() +
            "Constant: Scalar: [2.0], requireGradient: false" + System.lineSeparator() +
            " \t data: Scalar: [2.0]" + System.lineSeparator() +
            "ElementSum: Vector(1), requireGradient: true" + System.lineSeparator() +
            " \t data: Scalar: [12.0]" + System.lineSeparator() +
            "\t gradient: Scalar: [1.0]" + System.lineSeparator()
        );
    }
}
