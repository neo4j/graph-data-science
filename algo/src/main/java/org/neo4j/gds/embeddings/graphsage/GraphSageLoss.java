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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.ml.core.ComputationContext;
import org.neo4j.gds.ml.core.Dimensions;
import org.neo4j.gds.ml.core.RelationshipWeights;
import org.neo4j.gds.ml.core.Variable;
import org.neo4j.gds.ml.core.functions.Sigmoid;
import org.neo4j.gds.ml.core.functions.SingleParentVariable;
import org.neo4j.gds.ml.core.tensor.Matrix;
import org.neo4j.gds.ml.core.tensor.Scalar;

import java.util.stream.IntStream;

import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class GraphSageLoss extends SingleParentVariable<Matrix, Scalar> {

    // batch nodes (0), neighbor nodes(1), negative nodes(2)
    private static final int SAMPLING_BUCKETS = 3;
    private static final int NEGATIVE_NODES_OFFSET = 2;

    private final RelationshipWeights relationshipWeights;
    private final Variable<Matrix> combinedEmbeddings;
    private final long[] batch;
    private final int negativeSamplingFactor;

    // TODO: Pass this as configuration parameter.
    private static final double ALPHA = 1d;

    GraphSageLoss(
        RelationshipWeights relationshipWeights,
        Variable<Matrix> combinedEmbeddings,
        long[] batch,
        int negativeSamplingFactor
    ) {
        super(combinedEmbeddings, Dimensions.scalar());
        this.relationshipWeights = relationshipWeights;
        this.combinedEmbeddings = combinedEmbeddings;
        this.batch = batch;
        this.negativeSamplingFactor = negativeSamplingFactor;
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        Matrix embeddingData = ctx.data(combinedEmbeddings);
        // also, the offset for the neighbor nodes
        int bucketSize = embeddingData.rows() / SAMPLING_BUCKETS;
        int negativeNodesOffset = NEGATIVE_NODES_OFFSET * bucketSize;

        double loss = IntStream.range(0, bucketSize).mapToDouble(bucketIndex -> {
            int positiveNodeIdx = bucketIndex + bucketSize;
            int negativeNodeIdx = bucketIndex + negativeNodesOffset;
            double positiveAffinity = affinity(embeddingData, bucketIndex, positiveNodeIdx);
            double negativeAffinity = affinity(embeddingData, bucketIndex, negativeNodeIdx);

            return -relationshipWeightFactor(batch[bucketIndex], batch[positiveNodeIdx]) * Math.log(Sigmoid.sigmoid(positiveAffinity))
                   - negativeSamplingFactor * Math.log(Sigmoid.sigmoid(-negativeAffinity));
        }).sum();

        return new Scalar(loss / bucketSize);
    }

    private double relationshipWeightFactor(long nodeId, long positiveNodeId) {
        double relationshipWeight = relationshipWeights.weight(nodeId, positiveNodeId);

        // the positiveNode does not have to be a direct neighbor of that node
        if(Double.isNaN(relationshipWeight)) {
            relationshipWeight = RelationshipWeights.DEFAULT_VALUE;
        }

        return Math.pow(relationshipWeight, ALPHA);
    }

    private static double affinity(Matrix embeddingData, int batchIdx, int otherBatchIdx) {
        int embeddingDimension = embeddingData.cols();
        double sum = 0;
        for (int col = 0; col < embeddingDimension; col++) {
            sum += embeddingData.dataAt(batchIdx, col) * embeddingData.dataAt(otherBatchIdx, col);
        }
        return sum;
    }

    @Override
    public Matrix gradientForParent(ComputationContext ctx) {
        if (parent != combinedEmbeddings) {
            throw new IllegalStateException(formatWithLocale(
                "This variable only has a single parent. Expected %s but got %s",
                combinedEmbeddings,
                parent
            ));
        }

        Matrix embeddings = ctx.data(combinedEmbeddings);
        Matrix gradientResult = embeddings.createWithSameDimensions();


        int bucketSize = embeddings.rows() / SAMPLING_BUCKETS;
        int negativeNodesBucketOffset = NEGATIVE_NODES_OFFSET * bucketSize;
        int embeddingDimension = embeddings.cols();

        for (int bucketIdx = 0; bucketIdx < bucketSize; bucketIdx++) {
            int positiveNodeIdx = bucketIdx + bucketSize;
            int negativeNodeIdx = bucketIdx + negativeNodesBucketOffset;
            double positiveAffinity = affinity(embeddings, bucketIdx, positiveNodeIdx);
            double negativeAffinity = affinity(embeddings, bucketIdx, negativeNodeIdx);

            double relationshipWeightFactor = relationshipWeightFactor(batch[bucketIdx], batch[positiveNodeIdx]);
            double weightedPositiveLogistic = relationshipWeightFactor * logisticFunction(positiveAffinity);

            double weightedNegativeLogistic = negativeSamplingFactor * logisticFunction(-negativeAffinity);

            for (int embeddingIdx = 0; embeddingIdx < embeddingDimension; embeddingIdx++) {
                computeGradientForEmbeddingIdx(
                    embeddings,
                    gradientResult,
                    bucketIdx,
                    positiveNodeIdx,
                    negativeNodeIdx,
                    weightedPositiveLogistic,
                    weightedNegativeLogistic,
                    embeddingIdx
                );
            }
        }

        gradientResult.mapInPlace(i -> i / bucketSize);

        return gradientResult;
    }

    private static void computeGradientForEmbeddingIdx(
        Matrix embeddings,
        Matrix gradientResult,
        int batchIdx,
        int positiveNodeIdx,
        int negativeNodeIdx,
        double weightedPositiveLogistic,
        double weightedNegativeLogistic,
        int embeddingIdx
    ) {
        double scaledPositiveExampleGradient = -embeddings.dataAt(positiveNodeIdx, embeddingIdx) * weightedPositiveLogistic;
        double scaledNegativeExampleGradient = weightedNegativeLogistic * embeddings.dataAt(negativeNodeIdx, embeddingIdx);

        gradientResult.setDataAt(batchIdx, embeddingIdx, scaledPositiveExampleGradient + scaledNegativeExampleGradient);

        double currentEmbeddingValue = embeddings.dataAt(batchIdx, embeddingIdx);

        gradientResult.setDataAt(
            positiveNodeIdx,
            embeddingIdx,
            -currentEmbeddingValue * weightedPositiveLogistic
        );

        gradientResult.setDataAt(
            negativeNodeIdx,
            embeddingIdx,
            weightedNegativeLogistic * currentEmbeddingValue
        );
    }

    private static double logisticFunction(double affinity) {
        return 1 / (1 + Math.pow(Math.E, affinity));
    }
}
