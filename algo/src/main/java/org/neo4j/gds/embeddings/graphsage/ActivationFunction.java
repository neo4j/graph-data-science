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

import org.neo4j.gds.core.ml.Variable;
import org.neo4j.gds.core.ml.functions.Relu;
import org.neo4j.gds.core.ml.functions.Sigmoid;
import org.neo4j.gds.core.ml.tensor.Matrix;

import java.util.Locale;
import java.util.function.Function;

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

    public static ActivationFunction parse(Object object) {
        if (object == null) {
            return null;
        }
        if (object instanceof String) {
            return of(((String) object).toUpperCase(Locale.ENGLISH));
        }
        if (object instanceof ActivationFunction) {
            return (ActivationFunction) object;
        }
        return null;
    }

    public static String toString(ActivationFunction af) {
        return af.toString();
    }
}
