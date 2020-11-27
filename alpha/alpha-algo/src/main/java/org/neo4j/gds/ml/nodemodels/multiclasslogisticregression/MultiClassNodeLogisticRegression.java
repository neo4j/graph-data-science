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

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MultiClassCrossEntropyLoss;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Softmax;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.gds.ml.Batch;
import org.neo4j.gds.ml.Model;
import org.neo4j.graphalgo.api.Graph;
import org.neo4j.graphalgo.api.NodeProperties;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class MultiClassNodeLogisticRegression implements Model<Double> {
    private final String targetPropertyKey;
    private final Weights<Matrix> weights;
    private final LocalIdMap classIdMap;
    private final List<String> nodePropertyKeys;
    private final Graph graph;

    public MultiClassNodeLogisticRegression(
        List<String> nodeProperties,
        String targetPropertyKey,
        Graph graph
    ) {
        this.nodePropertyKeys = nodeProperties;
        this.graph = graph;
        this.targetPropertyKey = targetPropertyKey;
        classIdMap = new LocalIdMap();
        graph.forEachNode(nodeId -> {
            var targetClass = graph.nodeProperties(targetPropertyKey).doubleValue(nodeId);
            classIdMap.toMapped((long) targetClass);
            return true;
        });

        this.weights = initWeights(classIdMap.originalIds().length);
    }

    private Weights<Matrix> initWeights(int numberOfClasses) {
        var featuresPerClass = nodePropertyKeys.size() + 1;
        return new Weights<>(Matrix.fill(0.0, numberOfClasses, featuresPerClass));
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(weights);
    }

    @Override
    public Variable<Scalar> loss(Batch batch) {
        Iterable<Long> nodeIds = batch.nodeIds();
        int numberOfNodes = batch.size();
        MatrixConstant features = features(batch);
        Variable<Matrix> predictions = predictions(features, weights);
        double[] targets = new double[numberOfNodes];
        int nodeOffset = 0;
        for (long nodeId : nodeIds) {
            var v = graph.nodeProperties(targetPropertyKey).doubleValue(nodeId);
            targets[nodeOffset] = v;
            nodeOffset++;
        }
        MatrixConstant targetVariable = new MatrixConstant(targets, numberOfNodes, 1);
        return new MultiClassCrossEntropyLoss(predictions, targetVariable);
    }

    private Variable<Matrix> predictions(MatrixConstant features, Weights<Matrix> weights) {
        return new Softmax(MatrixMultiplyWithTransposedSecondOperand.of(features, weights));
    }

    private MatrixConstant features(Batch batch) {
        int numberOfNodes = batch.size();
        int nodePropertiesCount = nodePropertyKeys.size() + 1;
        double[] features = new double[numberOfNodes * nodePropertiesCount];
        for (int j = 0; j < nodePropertyKeys.size(); j++) {
            NodeProperties nodeProperties = graph.nodeProperties(nodePropertyKeys.get(j));
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

    @Override
    public List<Double> apply(Batch batch) {
        ComputationContext ctx = new ComputationContext();
        MatrixConstant features = features(batch);
        double[] data = ctx.forward(predictions(features, weights)).data();
        return Arrays.stream(data).boxed().collect(Collectors.toList());
    }
}
