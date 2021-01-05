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
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.LogisticLoss;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.ml.Batch;
import org.neo4j.gds.ml.Objective;
import org.neo4j.graphalgo.api.Graph;

import java.util.List;

import static org.neo4j.gds.ml.nodemodels.NodeFeaturesSupport.features;

public class NodeLogisticRegressionObjective implements Objective<NodeLogisticRegressionData> {
    private final String targetPropertyKey;
    private final Graph graph;

    private final NodeLogisticRegressionPredictor predictor;

    public NodeLogisticRegressionObjective(
        List<String> nodePropertyKeys,
        String targetPropertyKey,
        Graph graph
    ) {
        this.predictor = new NodeLogisticRegressionPredictor(makeData(nodePropertyKeys));
        this.targetPropertyKey = targetPropertyKey;
        this.graph = graph;
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(modelData().weights());
    }

    @Override
    public Variable<Scalar> loss(Batch batch, long trainSize) {
        var targets = makeTargets(batch);
        var features = features(graph, batch, modelData().nodePropertyKeys());
        var predictions = predictor.predictionsVariable(graph, batch);
        return new LogisticLoss(modelData().weights(), predictions, features, targets);
    }

    @Override
    public NodeLogisticRegressionData modelData() {
        return predictor.modelData();
    }

    private static NodeLogisticRegressionData makeData(
        List<String> nodePropertyKeys
    ) {
        return NodeLogisticRegressionData.builder()
            .weights(initWeights(nodePropertyKeys))
            .nodePropertyKeys(nodePropertyKeys)
            .build();
    }

    private static Weights<Matrix> initWeights(List<String> nodePropertyKeys) {
        double[] weights = new double[nodePropertyKeys.size() + 1];
        return new Weights<>(new Matrix(weights, 1, weights.length));
    }

    private MatrixConstant makeTargets(Batch batch) {
        Iterable<Long> nodeIds = batch.nodeIds();
        int rows = batch.size();
        double[] targets = new double[rows];
        int nodeOffset = 0;
        for (long nodeId : nodeIds) {
            targets[nodeOffset] = graph.nodeProperties(targetPropertyKey).doubleValue(nodeId);
            nodeOffset++;
        }
        return new MatrixConstant(targets, rows, 1);
    }
}
