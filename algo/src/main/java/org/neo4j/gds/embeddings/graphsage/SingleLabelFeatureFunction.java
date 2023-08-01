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

import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.collections.ha.HugeObjectArray;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.LazyConstant;
import org.neo4j.gds.ml.core.tensor.Matrix;

public class SingleLabelFeatureFunction implements FeatureFunction {

    @Override
    public Variable<Matrix> apply(
        Graph graph, long[] nodeIds, HugeObjectArray<double[]> features
    ) {
        int featureDimension = features.get(0).length;
        int[] dimension = {nodeIds.length, featureDimension};

        return new LazyConstant<>(() -> batchedFeatureExtractor(nodeIds, features, featureDimension), dimension);
    }

    @NotNull
    private Matrix batchedFeatureExtractor(
        long[] nodeIds,
        HugeObjectArray<double[]> features,
        int featureDimension
    ) {
        int batchLength = nodeIds.length;
        var batchFeatures = new Matrix(batchLength, featureDimension);

        for (int batchIdx = 0; batchIdx < batchLength; batchIdx++) {
            batchFeatures.setRow(batchIdx, features.get(nodeIds[batchIdx]));
        }

        return batchFeatures;
    }
}
