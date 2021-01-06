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
package org.neo4j.gds.ml.linkmodels.logisticregression;

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.Batch;
import org.neo4j.graphalgo.api.Graph;

public class LinkLogisticRegressionBase {

    protected final LinkLogisticRegressionData modelData;

    LinkLogisticRegressionBase(LinkLogisticRegressionData modelData) {
        this.modelData = modelData;
    }

    protected Variable<Matrix> predictions(MatrixConstant features) {
        return new Sigmoid<>(MatrixMultiplyWithTransposedSecondOperand.of(features, modelData.weights()));
    }

    protected MatrixConstant features(Graph graph, Batch batch) {
        var graphCopy = graph.concurrentCopy();
        var relationshipCount = new MutableInt();
        // assume batching has been done so that relationship count does not overflow int
        batch.nodeIds().forEach(nodeId -> relationshipCount.add(graph.degree(nodeId)));
        int rows = relationshipCount.intValue();
        int cols = modelData.numberOfFeatures();
        double[] features = new double[rows * cols];
        var relationshipOffset = new MutableInt();
        batch.nodeIds().forEach(nodeId -> {
            graphCopy.forEachRelationship(nodeId, (src, trg) -> {
                var sourceFeatures = nodeFeatures(graph, src);
                var targetFeatures = nodeFeatures(graph, trg);
                var linkFeatures = modelData.linkFeatureCombiner().combine(sourceFeatures, targetFeatures);
                setLinkFeatures(linkFeatures, features, relationshipOffset.getValue());
                relationshipOffset.increment();
                return true;
            });
        });
        for (int nodeOffset = 0; nodeOffset < rows; nodeOffset++) {
            features[nodeOffset * cols + cols - 1] = 1.0;
        }
        return new MatrixConstant(features, rows, cols);
    }

    private double[] nodeFeatures(Graph graph, long nodeId) {
        int numberOfFeatures = modelData.numberOfFeatures();
        var features = new double[numberOfFeatures];
        var featuresProcessed = new MutableInt();
        modelData.nodePropertyKeys().forEach(propertyKey -> {
            var nodeProperties = graph.nodeProperties(propertyKey);
            switch (nodeProperties.valueType()) {
                case DOUBLE_ARRAY:
                    var propertyArray = nodeProperties.doubleArrayValue(nodeId);
                    System.arraycopy(propertyArray, 0, features, featuresProcessed.getValue(), propertyArray.length);
                    featuresProcessed.add(propertyArray.length);
                    break;
                case DOUBLE:
                    var propertyValue = nodeProperties.doubleValue(nodeId);
                    features[featuresProcessed.getValue()] = propertyValue;
                    featuresProcessed.increment();
                    break;
                default:
                    throw new IllegalStateException(
                        "Link Logistic Regression requires double or double array node properties, not "
                        + nodeProperties.valueType()
                    );
            }
        });
        return features;
    }

    private void setLinkFeatures(double[] linkFeatures, double[] features, int relationshipOffset) {
        var numberOfFeatures = linkFeatures.length;
        System.arraycopy(linkFeatures, 0, features, relationshipOffset * numberOfFeatures, numberOfFeatures);
    }

}
