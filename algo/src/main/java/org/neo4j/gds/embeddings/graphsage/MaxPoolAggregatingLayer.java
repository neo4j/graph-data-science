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

import org.neo4j.gds.ml.core.functions.Weights;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Vector;

public class MaxPoolAggregatingLayer implements Layer {

    private final int sampleSize;
    private final Weights<Matrix> poolWeights;
    private final Weights<Matrix> selfWeights;
    private final Weights<Matrix> neighborsWeights;
    private final Weights<Vector> bias;
    private final ActivationFunction activationFunction;

    public MaxPoolAggregatingLayer(
        int sampleSize,
        Weights<Matrix> poolWeights,
        Weights<Matrix> selfWeights,
        Weights<Matrix> neighborsWeights,
        Weights<Vector> bias,
        ActivationFunction activationFunction
    ) {
        this.poolWeights = poolWeights;
        this.selfWeights = selfWeights;
        this.neighborsWeights = neighborsWeights;
        this.bias = bias;
        this.sampleSize = sampleSize;
        this.activationFunction = activationFunction;
    }

    @Override
    public int sampleSize() {
        return sampleSize;
    }

    @Override
    public Aggregator aggregator() {
        return new MaxPoolingAggregator(
            this.poolWeights,
            this.selfWeights,
            this.neighborsWeights,
            this.bias,
            activationFunction
        );
    }
}
