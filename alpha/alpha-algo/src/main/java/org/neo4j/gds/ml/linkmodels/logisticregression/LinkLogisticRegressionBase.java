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
import org.jetbrains.annotations.NotNull;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixConstant;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.MatrixMultiplyWithTransposedSecondOperand;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.ml.batch.Batch;
import org.neo4j.gds.ml.features.FeatureConsumer;
import org.neo4j.gds.ml.features.FeatureExtraction;
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
        // TODO: replace by MutableLong and throw an error saying reduce batchSize if larger than maxint
        var relationshipCount = new MutableInt();
        // assume batching has been done so that relationship count does not overflow int
        batch.nodeIds().forEach(nodeId -> relationshipCount.add(graph.degree(nodeId)));
        int rows = relationshipCount.intValue();
        int cols = modelData.numberOfFeatures();
        double[] features = new double[rows * cols];
        var relationshipOffset = new MutableInt();
        batch.nodeIds().forEach(nodeId -> {
            graphCopy.forEachRelationship(nodeId, (src, trg) -> {
                var linkFeatures = features(graph, src, trg);
                setLinkFeatures(linkFeatures, features, relationshipOffset.getValue(), cols);
                relationshipOffset.increment();
                return true;
            });
        });
        return new MatrixConstant(features, rows, cols);
    }

    protected double[] features(Graph graph, long sourceId, long targetId) {
        var sourceFeatures = nodeFeatures(graph, sourceId);
        var targetFeatures = nodeFeatures(graph, targetId);
        return modelData.linkFeatureCombiner().combine(sourceFeatures, targetFeatures);
    }

    protected double[] nodeFeatures(Graph graph, long nodeId) {
        var features = new double[modelData.numberOfNodeFeatures()];

        var consumer = featureConsumer(features);
        FeatureExtraction.extract(
            nodeId,
            -1,
            FeatureExtraction.propertyExtractors(graph, modelData.featureProperties()),
            consumer
        );
        return features;
    }

    @NotNull
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
