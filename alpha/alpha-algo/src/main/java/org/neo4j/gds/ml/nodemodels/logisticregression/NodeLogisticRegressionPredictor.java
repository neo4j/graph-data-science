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
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Softmax;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.Predictor;
import org.neo4j.gds.ml.batch.Batch;
import org.neo4j.gds.ml.features.BiasFeature;
import org.neo4j.gds.ml.features.FeatureExtraction;
import org.neo4j.gds.ml.features.FeatureExtractor;
import org.neo4j.graphalgo.api.Graph;

import java.util.ArrayList;
import java.util.List;

import static org.neo4j.gds.ml.features.FeatureExtraction.extract;
import static org.neo4j.graphalgo.core.utils.mem.MemoryUsage.sizeOfInstance;

public class NodeLogisticRegressionPredictor implements Predictor<Matrix, NodeLogisticRegressionData> {

    private final NodeLogisticRegressionData modelData;
    private final List<String> featureProperties;

    /**
     * Memory usage for this guy is initially zero - it just holds fields
     *
     * It then gets called, `predict(batch)` stylee, where it constructs some small (batch-size) structures that will cost memory
     *
     * That memory needs to be accounted for by calling code I reckon
     */
    public static long sizeOfPredictionsVariableInBytes(int batchSize, int numberOfFeatures) {
        return
            sizeOfFeatureExtractorsInBytes(numberOfFeatures) +
            MatrixConstant.sizeInBytes(batchSize, numberOfFeatures) +
            MatrixMultiplyWithTransposedSecondOperand.sizeInBytes(batchSize, weights); // this is where we got to

//            .add("softmax", Softmax.memoryEstimation())
    }

    private static long sizeOfFeatureExtractorsInBytes(int numberOfFeatures) {
        return FeatureExtraction.memoryUsageInBytes(numberOfFeatures) + sizeOfInstance(BiasFeature.class);
    }

    public NodeLogisticRegressionPredictor(NodeLogisticRegressionData modelData, List<String> featureProperties) {
        this.modelData = modelData;
        this.featureProperties = featureProperties;
    }

    @Override
    public NodeLogisticRegressionData modelData() {
        return modelData;
    }

    @Override
    public Matrix predict(Graph graph, Batch batch) {
        ComputationContext ctx = new ComputationContext();
        return ctx.forward(predictionsVariable(graph, batch));
    }

    Variable<Matrix> predictionsVariable(Graph graph, Batch batch) {
        MatrixConstant features = features(graph, batch);
        var weights = modelData.weights();
        return new Softmax(MatrixMultiplyWithTransposedSecondOperand.of(features, weights));
    }

    private MatrixConstant features(Graph graph, Batch batch) {
        var featureExtractors = featureExtractors(graph);
        return extract(batch, featureExtractors);
    }

    private List<FeatureExtractor> featureExtractors(Graph graph) {
        var featureExtractors = new ArrayList<FeatureExtractor>();
        featureExtractors.addAll(FeatureExtraction.propertyExtractors(graph, featureProperties));
        featureExtractors.add(new BiasFeature());
        return featureExtractors;
    }
}
