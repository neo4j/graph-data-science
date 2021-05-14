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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.Relu;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.graphalgo.utils.StringJoining;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.utils.StringFormatting.toUpperCaseWithLocale;

public enum ActivationFunction {
    SIGMOID {
        @Override
        public Function<Variable<Matrix>, Variable<Matrix>> activationFunction() {
            return Sigmoid::new;
        }

        @Override
        public double weightInitBound(int rows, int cols) {
            return Math.sqrt(2d / (rows + cols));
        }
    },
    RELU {
        @Override
        public Function<Variable<Matrix>, Variable<Matrix>> activationFunction() {
            return Relu::new;
        }

        @Override
        public double weightInitBound(int rows, int cols) {
            return Math.sqrt(2d / cols);
        }
    };

    public abstract Function<Variable<Matrix>, Variable<Matrix>> activationFunction();

    public abstract double weightInitBound(int rows, int cols);

    public static ActivationFunction of(String activationFunction) {
        return valueOf(toUpperCaseWithLocale(activationFunction));
    }

    private static final List<String> VALUES = Arrays
        .stream(ActivationFunction.values())
        .map(ActivationFunction::name)
        .collect(Collectors.toList());


    public static ActivationFunction parse(Object input) {
        if (input instanceof String) {
            var inputString = toUpperCaseWithLocale((String) input);

            if (!VALUES.contains(inputString)) {
                throw new IllegalArgumentException(formatWithLocale(
                    "ActivationFunction `%s` is not supported. Must be one of: %s.",
                    input,
                    StringJoining.join(VALUES)
                ));
            }

            return of(inputString);
        } else if (input instanceof ActivationFunction) {
            return (ActivationFunction) input;
        }

        throw new IllegalArgumentException(formatWithLocale(
            "Expected ActivationFunction or String. Got %s.",
            input.getClass().getSimpleName()
        ));
    }

    public static String toString(ActivationFunction af) {
        return af.toString();
    }
}
