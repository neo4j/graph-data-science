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
package org.neo4j.gds.ml.nodemodels.multiclasslogisticregression;

import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Softmax;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.Batch;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;

public class MultiClassNodeLogisticRegressionBase {

    protected final MultiClassNodeLogisticRegressionData modelData;

    MultiClassNodeLogisticRegressionBase(MultiClassNodeLogisticRegressionData modelData) {
        this.modelData = modelData;
    }

    protected Variable<Matrix> predictions(MatrixConstant features, Weights<Matrix> weights) {
        return new Softmax(MatrixMultiplyWithTransposedSecondOperand.of(features, weights));
    }

    protected MatrixConstant features(Graph graph, Batch batch) {
        int numberOfNodes = batch.size();
        int nodePropertiesCount = modelData.nodePropertyKeys().size() + 1;
        double[] features = new double[numberOfNodes * nodePropertiesCount];
        for (int j = 0; j < modelData.nodePropertyKeys().size(); j++) {
            NodeProperties nodeProperties = graph.nodeProperties(modelData.nodePropertyKeys().get(j));
            int nodeOffset = 0;
            for (long nodeId : batch.nodeIds()) {
                features[nodeOffset * nodePropertiesCount + j] = nodeProperties.doubleValue(nodeId);
                nodeOffset++;
            }
        }
        for (int nodeOffset = 0; nodeOffset < batch.size(); nodeOffset++) {
            features[nodeOffset * nodePropertiesCount + nodePropertiesCount - 1] = 1.0;
        }
        return new MatrixConstant(features, numberOfNodes, nodePropertiesCount);
    }
}
