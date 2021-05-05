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
package org.neo4j.gds.ml.linkmodels.logisticregression;

import org.eclipse.collections.impl.tuple.Tuples;
import org.neo4j.gds.ml.core.functions.MatrixConstant;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.graphalgo.api.Graph;

import java.util.List;

public class LinkLogisticRegressionPredictor extends LinkLogisticRegressionBase {

    public LinkLogisticRegressionPredictor(LinkLogisticRegressionData modelData, List<String> featureProperties) {
        super(modelData, featureProperties);
    }

    static long sizeOfBatchInBytes(int batchSize, int numberOfFeatures) {
        var dimensionsOfFirstMatrix = Tuples.pair(batchSize, numberOfFeatures);
        var dimensionsOfSecondMatrix = Tuples.pair(1, numberOfFeatures);
        var resultRows = dimensionsOfFirstMatrix.getOne();
        return
            sizeOfFeatureExtractorsInBytes(numberOfFeatures) +
            MatrixConstant.sizeInBytes(batchSize, numberOfFeatures) +
            MatrixMultiplyWithTransposedSecondOperand.sizeInBytes(dimensionsOfFirstMatrix, dimensionsOfSecondMatrix) +
            Sigmoid.sizeInBytes(resultRows, 1);
    }

    private static long sizeOfFeatureExtractorsInBytes(int numberOfFeatures) {
        return FeatureExtraction.memoryUsageInBytes(numberOfFeatures);
    }

    public LinkLogisticRegressionData modelData() {
        return modelData;
    }

    public double predictedProbability(Graph graph, long sourceId, long targetId) {
        var weightsArray = modelData.weights().data().data();
        var features = features(graph, sourceId, targetId);
        var affinity = 0D;
        var biasIndex = weightsArray.length - 1;
        for (int i = 0; i < biasIndex; i++) {
            affinity += weightsArray[i] * features[i];
        }
        var bias = weightsArray[biasIndex];
        return Sigmoid.sigmoid(affinity + bias);
    }
}
