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

import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.gds.api.Graph;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.batch.Batch;
import org.neo4j.gds.ml.core.features.FeatureConsumer;
import org.neo4j.gds.ml.core.features.FeatureExtraction;
import org.neo4j.gds.ml.core.features.FeatureExtractor;
import org.neo4j.gds.ml.core.functions.Constant;
import org.neo4j.gds.ml.core.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.tensor.Matrix;

import java.util.List;

public class LinkLogisticRegressionBase {

    protected final LinkLogisticRegressionData modelData;
    protected final List<FeatureExtractor> extractors;

    LinkLogisticRegressionBase(
        LinkLogisticRegressionData modelData,
        List<FeatureExtractor> extractors
    ) {
        this.modelData = modelData;
        this.extractors = extractors;
    }

    protected Variable<Matrix> predictions(Constant<Matrix> features) {
        return new Sigmoid<>(MatrixMultiplyWithTransposedSecondOperand.of(features, modelData.weights()));
    }

    protected Constant<Matrix> features(Graph graph, Batch batch, int rows) {
        var graphCopy = graph.concurrentCopy();
        int cols = modelData.linkFeatureDimension();
        double[] features = new double[rows * cols];
        var relationshipOffset = new MutableInt();
        batch.nodeIds().forEach(nodeId -> {
            graphCopy.forEachRelationship(nodeId, (src, trg) -> {
                var linkFeatures = features(src, trg);
                setLinkFeatures(linkFeatures, features, relationshipOffset.getValue(), cols);
                relationshipOffset.increment();
                return true;
            });
        });
        return Constant.matrix(features, rows, cols);
    }

    protected double[] features(long sourceId, long targetId) {
        var sourceFeatures = nodeFeatures(sourceId);
        var targetFeatures = nodeFeatures(targetId);
        return modelData.linkFeatureCombiner().combine(sourceFeatures, targetFeatures);
    }

    protected double[] nodeFeatures(long nodeId) {
        var features = new double[modelData.nodeFeatureDimension()];

        var consumer = featureConsumer(features);
        FeatureExtraction.extract(
            nodeId,
            -1,
            extractors,
            consumer
        );
        return features;
    }

    private FeatureConsumer featureConsumer(double[] features) {
        return new FeatureConsumer() {
            @Override
            public void acceptScalar(long nodeOffset, int offset, double value) {
                features[offset] = value;
            }

            @Override
            public void acceptArray(long nodeOffset, int offset, double[] values) {
                System.arraycopy(values, 0, features, offset, values.length);
            }
        };
    }

    private void setLinkFeatures(double[] linkFeatures, double[] features, int relationshipOffset, int cols) {
        System.arraycopy(linkFeatures, 0, features, relationshipOffset * cols, cols);
    }
}
