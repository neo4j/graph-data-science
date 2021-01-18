/*
 * Copyright (c) 2017-2021 "Neo4j,"
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
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.ConstantScale;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.ElementSum;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.L2NormSquared;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MultiClassCrossEntropyLoss;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Weights;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;
import org.neo4j.gds.embeddings.graphsage.subgraph.LocalIdMap;
import org.neo4j.gds.ml.Batch;
import org.neo4j.gds.ml.Objective;
import org.neo4j.graphalgo.api.Graph;

import java.util.List;

public class MultiClassNLRObjective implements Objective<MultiClassNLRData> {

    private final String targetPropertyKey;
    private final Graph graph;
    private final double penalty;
    private final MultiClassNLRPredictor predictor;

    public MultiClassNLRObjective(
        List<String> featureProperties,
        String targetPropertyKey,
        Graph graph,
        double penalty
    ) {
        this.predictor = new MultiClassNLRPredictor(makeData(
            featureProperties,
            targetPropertyKey,
            graph
        ), featureProperties);
        this.targetPropertyKey = targetPropertyKey;
        this.graph = graph;
        this.penalty = penalty;
    }

    private static MultiClassNLRData makeData(
        List<String> featureProperties,
        String targetPropertyKey,
        Graph graph
    ) {
        var classIdMap = makeClassIdMap(targetPropertyKey, graph);
        return MultiClassNLRData.builder()
            .classIdMap(classIdMap)
            .weights(initWeights(featureProperties, classIdMap.originalIds().length))
            .build();
    }

    private static LocalIdMap makeClassIdMap(String targetPropertyKey, Graph graph) {
        var classIdMap = new LocalIdMap();
        graph.forEachNode(nodeId -> {
            var targetClass = graph.nodeProperties(targetPropertyKey).doubleValue(nodeId);
            classIdMap.toMapped((long) targetClass);
            return true;
        });
        return classIdMap;
    }

    private static Weights<Matrix> initWeights(List<String> featureProperties, int numberOfClasses) {
        var featuresPerClass = featureProperties.size() + 1;
        return new Weights<>(Matrix.fill(0.0, numberOfClasses, featuresPerClass));
    }

    @Override
    public List<Weights<? extends Tensor<?>>> weights() {
        return List.of(modelData().weights());
    }

    @Override
    public Variable<Scalar> loss(Batch batch, long trainSize) {
        var targets = makeTargets(batch);
        var predictions = predictor.predictionsVariable(graph, batch);
        var unpenalizedLoss = new MultiClassCrossEntropyLoss(
            predictions,
            targets
        );
        var penaltyVariable = new ConstantScale<>(new L2NormSquared(modelData().weights()), batch.size() * penalty / trainSize);
        return new ElementSum(List.of(unpenalizedLoss, penaltyVariable));
    }

    private MatrixConstant makeTargets(Batch batch) {
        Iterable<Long> nodeIds = batch.nodeIds();
        int numberOfNodes = batch.size();
        double[] targets = new double[numberOfNodes];
        int nodeOffset = 0;
        var localIdMap = modelData().classIdMap();
        var targetNodeProperty = graph.nodeProperties(targetPropertyKey);
        for (long nodeId : nodeIds) {
            var targetValue = targetNodeProperty.doubleValue(nodeId);
            targets[nodeOffset] = localIdMap.toMapped((long) targetValue);
            nodeOffset++;
        }
        return new MatrixConstant(targets, numberOfNodes, 1);
    }

    @Override
    public MultiClassNLRData modelData() {
        return predictor.modelData();
    }
}
