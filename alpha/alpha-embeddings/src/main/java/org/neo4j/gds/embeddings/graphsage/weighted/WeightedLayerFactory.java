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
package org.neo4j.gds.embeddings.graphsage.weighted;

import org.neo4j.gds.embeddings.graphsage.ActivationFunction;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.graphalgo.api.Graph;

import java.util.concurrent.ThreadLocalRandom;

import static org.neo4j.graphalgo.utils.StringFormatting.formatWithLocale;

public final class WeightedLayerFactory {
    private WeightedLayerFactory() {}

    public static WeightedLayer createLayer(Graph graph, WeightedLayerConfig layerConfig) {
        int rows = layerConfig.rows();
        int cols = layerConfig.cols();

        ActivationFunction activationFunction = layerConfig.activationFunction();

        Weights<Matrix> weights = generateWeights(
            rows,
            cols,
            activationFunction.weightInitBound(rows, cols)
        );

        if (layerConfig.aggregatorType() == Aggregator.AggregatorType.WEIGHTED_MEAN) {
            return new WeightedMeanAggregatingLayer(
                graph,
                weights,
                layerConfig.sampleSize(),
                activationFunction.activationFunction()
            );
        }

        throw new RuntimeException(formatWithLocale(
            "Aggregator: %s is not applicable to %s",
            layerConfig.aggregatorType(),
            WeightedLayerFactory.class.getSimpleName()
        ));
    }

    private static Weights<Matrix> generateWeights(int rows, int cols, double weightBound) {

        double[] data = ThreadLocalRandom.current()
            .doubles(rows * cols, -weightBound, weightBound)
            .toArray();

        return new Weights<>(new Matrix(
            data,
            rows,
            cols
        ));
    }

}
