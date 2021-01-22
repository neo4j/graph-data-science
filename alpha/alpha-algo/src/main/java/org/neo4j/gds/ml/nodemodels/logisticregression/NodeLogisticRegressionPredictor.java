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
package org.neo4j.gds.ml.nodemodels.logisticregression;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.Batch;
import org.neo4j.gds.ml.Predictor;
import org.neo4j.graphalgo.api.Graph;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.neo4j.gds.ml.nodemodels.NodeFeaturesSupport.features;

public class NodeLogisticRegressionPredictor implements Predictor<List<Double>, NodeLogisticRegressionData> {

    private final NodeLogisticRegressionData modelData;

    NodeLogisticRegressionPredictor(NodeLogisticRegressionData modelData) {
        this.modelData = modelData;
    }

    @Override
    public NodeLogisticRegressionData modelData() {
        return modelData;
    }

    @Override
    public List<Double> predict(Graph graph, Batch batch) {
        ComputationContext ctx = new ComputationContext();
        double[] data = ctx.forward(predictionsVariable(graph, batch)).data();
        return Arrays.stream(data).boxed().collect(Collectors.toList());
    }

    Variable<Matrix> predictionsVariable(Graph graph, Batch batch) {
        var features = features(graph, batch, modelData.nodePropertyKeys());
        var weights = modelData.weights();
        return new Sigmoid<>(MatrixMultiplyWithTransposedSecondOperand.of(features, weights));
    }
}
