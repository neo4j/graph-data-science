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
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;

import java.util.concurrent.ThreadLocalRandom;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class LayerFactory {
    private LayerFactory() {}

    public static Layer createLayer(LayerConfig layerConfig) {
        if (layerConfig.aggregatorType() == Aggregator.AggregatorType.MEAN) {
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

        if (layerConfig.aggregatorType() == Aggregator.AggregatorType.POOL) {
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

        throw new RuntimeException(formatWithLocale("Aggregator: %s is unknown", layerConfig.aggregatorType()));
    }

    private static Weights generateWeights(int rows, int cols, double weightBound) {

        double[] data = ThreadLocalRandom.current()
            .doubles(rows * cols, -weightBound, weightBound)
            .toArray();

        return new Weights(Tensor.matrix(
            data,
            rows,
            cols
        ));
    }

}
