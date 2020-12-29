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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.Batch;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;

public class NodeLogisticRegressionBase {

    protected final NodeLogisticRegressionData modelData;

    NodeLogisticRegressionBase(NodeLogisticRegressionData modelData) {
        this.modelData = modelData;
    }

    protected Variable<Matrix> predictions(MatrixConstant features) {
        return new Sigmoid<>(MatrixMultiplyWithTransposedSecondOperand.of(features, modelData.weights()));
    }

    protected MatrixConstant features(Graph graph, Batch batch) {
        int rows = batch.size();
        var nodePropertyKeys = modelData.nodePropertyKeys();
        int cols = nodePropertyKeys.size() + 1;
        double[] features = new double[rows * cols];
        for (int j = 0; j < nodePropertyKeys.size(); j++) {
            NodeProperties nodeProperties = graph.nodeProperties(nodePropertyKeys.get(j));
            int nodeOffset = 0;
            for (long nodeId : batch.nodeIds()) {
                features[nodeOffset * cols + j] = nodeProperties.doubleValue(nodeId);
                nodeOffset++;
            }
        }
        for (int nodeOffset = 0; nodeOffset < batch.size(); nodeOffset++) {
            features[nodeOffset * cols + cols - 1] = 1.0;
        }
        return new MatrixConstant(features, rows, cols);
    }

}
