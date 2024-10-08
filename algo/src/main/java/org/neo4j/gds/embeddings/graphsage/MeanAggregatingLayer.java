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

public class MeanAggregatingLayer implements Layer {

    private final int sampleSize;
    private final Weights<Matrix> weights;
    private final ActivationFunctionWrapper activationFunctionWrapper;

    public MeanAggregatingLayer(
        Weights<Matrix> weights,
        int sampleSize,
        ActivationFunctionWrapper activationFunctionWrapper
    ) {
        this.sampleSize = sampleSize;
        this.weights = weights;
        this.activationFunctionWrapper = activationFunctionWrapper;
    }

    @Override
    public Aggregator aggregator() {
        return new MeanAggregator(weights, activationFunctionWrapper);
    }

    @Override
    public int sampleSize() {
        return sampleSize;
    }
}
