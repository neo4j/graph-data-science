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

import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Relu;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;

import java.util.Locale;
import java.util.Random;
import java.util.function.Function;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;
import static org.neo4j.graphalgo.utils.StringFormatting.toLowerCaseWithLocale;
import static org.neo4j.graphalgo.utils.StringFormatting.toUpperCaseWithLocale;

public final class LayerInitialisationFactory {
    private LayerInitialisationFactory() {}

    public static Layer createLayer(LayerConfig layerConfig) {
        String aggregatorType = layerConfig.aggregatorType();

        if ("mean".equals(toLowerCaseWithLocale(aggregatorType))) {
            int rows = layerConfig.rows();
            int cols = layerConfig.cols();

            ActivationFunction activationFunction = layerConfig.activationFunction();

            Weights weights = generateWeights(
                rows,
                cols,
                activationFunction.weightInitBound(rows, cols)
            );
            return new MeanAggregatingLayer(weights, layerConfig.sampleSize(), activationFunction.activationFunction());
        }

        if ("pool".equals(toLowerCaseWithLocale(aggregatorType))) {
            int rows = layerConfig.rows();
            int cols = layerConfig.cols();

            ActivationFunction activationFunction = layerConfig.activationFunction();

            Weights poolWeights = generateWeights(
                rows,
                cols,
                activationFunction.weightInitBound(rows, cols)
            );

            Weights selfWeights = generateWeights(
                rows,
                cols,
                activationFunction.weightInitBound(rows, cols)
            );

            Weights neighborsWeights = generateWeights(
                rows,
                rows,
                activationFunction.weightInitBound(rows, rows)
            );

            Weights bias = new Weights(Tensor.constant(0D, Dimensions.vector(rows)));

            return new MaxPoolAggregatingLayer(
                layerConfig.sampleSize(),
                poolWeights,
                selfWeights,
                neighborsWeights,
                bias,
                activationFunction.activationFunction()
            );
        }

        throw new RuntimeException(formatWithLocale("Aggregator: %s is unknown", aggregatorType));
    }

    private static Weights generateWeights(int rows, int cols, double weightBound) {

        double[] data = new Random()
            .doubles(rows * cols, -weightBound, weightBound)
            .toArray();

        return new Weights(Tensor.matrix(
            data,
            rows,
            cols
        ));
    }

    public enum ActivationFunction {
        SIGMOID {
            @Override
            public Function<Variable, Variable> activationFunction() {
                return Sigmoid::new;
            }

            @Override
            public double weightInitBound(int rows, int cols) {
                return Math.sqrt(2d / (rows + cols));
            }
        },
        RELU {
            @Override
            public Function<Variable, Variable> activationFunction() {
                return Relu::new;
            }

            @Override
            public double weightInitBound(int rows, int cols) {
                return Math.sqrt(2d / cols);
            }
        };

        public abstract Function<Variable, Variable> activationFunction();

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
}
