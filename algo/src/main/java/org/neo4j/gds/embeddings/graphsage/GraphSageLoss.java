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

import static org.neo4j.gds.ml.core.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.utils.StringFormatting.formatWithLocale;

public class GraphSageLoss extends SingleParentVariable<Scalar> {

    private static final int NEGATIVE_NODES_OFFSET = 2;
    private static final int SAMPLING_BUCKETS = 3;

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
        int batchSize = embeddingData.rows() / SAMPLING_BUCKETS;
        int negativeNodesOffset = NEGATIVE_NODES_OFFSET * batchSize;

        double loss = IntStream.range(0, batchSize).mapToDouble(nodeIdOffset -> {
            int positiveNodeOffset = nodeIdOffset + batchSize;
            int negativeNodeOffset = nodeIdOffset + negativeNodesOffset;
            double positiveAffinity = affinity(embeddingData, nodeIdOffset, positiveNodeOffset);
            double negativeAffinity = affinity(embeddingData, nodeIdOffset, negativeNodeOffset);

            return -relationshipWeightFactor(batch[nodeIdOffset], batch[positiveNodeOffset]) * Math.log(Sigmoid.sigmoid(positiveAffinity))
                   - negativeSamplingFactor * Math.log(Sigmoid.sigmoid(-negativeAffinity));
        }).sum();
        return new Scalar(loss);
    }

    private double relationshipWeightFactor(long nodeId, long positiveNodeId) {
        double relationshipWeight = relationshipWeights.weight(nodeId, positiveNodeId);
        if(Double.isNaN(relationshipWeight)) {
            relationshipWeight = RelationshipWeights.DEFAULT_VALUE;
        }
        return Math.pow(relationshipWeight, ALPHA);
    }

    private double affinity(Matrix embeddingData, int nodeIdOffset, int otherNodeIdOffset) {
        int embeddingDimension = combinedEmbeddings.dimension(COLUMNS_INDEX);
        double sum = 0;
        for (int i = 0; i < embeddingDimension; i++) {
            sum += embeddingData.dataAt(nodeIdOffset, i) * embeddingData.dataAt(otherNodeIdOffset, i);
        }
        return sum;
    }

    @Override
    public Matrix gradient(Variable<?> parent, ComputationContext ctx) {
        if (parent != combinedEmbeddings) {
            throw new IllegalStateException(formatWithLocale(
                "This variable only has a single parent. Expected %s but got %s",
                combinedEmbeddings,
                parent
            ));
        }

        Matrix embeddings = ctx.data(combinedEmbeddings);
        Matrix gradientResult = embeddings.createWithSameDimensions();


        int nodeBatchSize = embeddings.rows() / SAMPLING_BUCKETS;
        int negativeNodesOffset = NEGATIVE_NODES_OFFSET * nodeBatchSize;
        int embeddingDimension = embeddings.cols();

        for (int nodeOffset = 0; nodeOffset < nodeBatchSize; nodeOffset++) {
            int positiveNodeOffset = nodeOffset + nodeBatchSize;
            int negativeNodeOffset = nodeOffset + negativeNodesOffset;
            double positiveAffinity = affinity(embeddings, nodeOffset, positiveNodeOffset);
            double negativeAffinity = affinity(embeddings, nodeOffset, negativeNodeOffset);

            double positiveLogistic = logisticFunction(positiveAffinity);
            double negativeLogistic = logisticFunction(-negativeAffinity);

            for (int embeddingIdx = 0; embeddingIdx < embeddingDimension; embeddingIdx++) {
                computeGradientForEmbeddingIdx(
                    embeddings,
                    gradientResult,
                    nodeOffset,
                    positiveNodeOffset,
                    negativeNodeOffset,
                    positiveLogistic,
                    negativeLogistic,
                    embeddingIdx
                );
            }

        }
        return gradientResult;
    }

    private void computeGradientForEmbeddingIdx(
        Matrix embeddings,
        Matrix gradientResult,
        int nodeOffset,
        int positiveNodeOffset,
        int negativeNodeOffset,
        double positiveLogistic,
        double negativeLogistic,
        int embeddingIdx
    ) {
        double relationshipWeightFactor = relationshipWeightFactor(batch[nodeOffset], batch[positiveNodeOffset]);
        double weightedPositiveLogistic = relationshipWeightFactor * positiveLogistic;

        double scaledPositiveExampleGradient = - embeddings.dataAt(positiveNodeOffset, embeddingIdx) * weightedPositiveLogistic;
        double scaledNegativeExampleGradient = negativeSamplingFactor * embeddings.dataAt(negativeNodeOffset, embeddingIdx) * negativeLogistic;

        gradientResult.setDataAt(nodeOffset, embeddingIdx, scaledPositiveExampleGradient + scaledNegativeExampleGradient);

        double currentEmbeddingValue = embeddings.dataAt(nodeOffset, embeddingIdx);

        gradientResult.setDataAt(
            positiveNodeOffset,
            embeddingIdx,
            -currentEmbeddingValue * weightedPositiveLogistic
        );

        gradientResult.setDataAt(
            negativeNodeOffset,
            embeddingIdx,
            negativeSamplingFactor * currentEmbeddingValue * negativeLogistic
        );
    }

    private double logisticFunction(double affinity) {
        return 1 / (1 + Math.pow(Math.E, affinity));
    }
}
