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
import org.neo4j.gds.ml.core.tensor.Tensor;

import java.util.stream.IntStream;

import static org.neo4j.gds.ml.core.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.ml.core.Dimensions.ROWS_INDEX;

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
        Tensor<?> embeddingData = ctx.data(parent());
        int batchSize = embeddingData.dimension(ROWS_INDEX) / SAMPLING_BUCKETS;
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

    private double affinity(Tensor<?> embeddingData, int nodeIdOffset, int otherNodeIdOffset) {
        int embeddingDimension = combinedEmbeddings.dimension(COLUMNS_INDEX);
        double sum = 0;
        int embeddingOffset = nodeIdOffset * embeddingDimension;
        int otherEmbeddingOffset = otherNodeIdOffset * embeddingDimension;
        for (int i = 0; i < embeddingDimension; i++) {
            sum += embeddingData.dataAt(embeddingOffset + i) * embeddingData.dataAt(otherEmbeddingOffset + i);
        }
        return sum;
    }

    @Override
    public Matrix gradient(Variable<?> parent, ComputationContext ctx) {
        Tensor<?> embeddings = ctx.data(parent);
        int totalBatchSize = embeddings.dimension(ROWS_INDEX);
        int nodeBatchSize = totalBatchSize / SAMPLING_BUCKETS;

        int embeddingDimension = embeddings.dimension(COLUMNS_INDEX);
        Matrix gradientResult = new Matrix(totalBatchSize, embeddingDimension);

        int negativeNodesOffset = NEGATIVE_NODES_OFFSET * nodeBatchSize;
        IntStream.range(0, nodeBatchSize).forEach(nodeOffset -> {
            int positiveNodeOffset = nodeOffset + nodeBatchSize;
            int negativeNodeOffset = nodeOffset + negativeNodesOffset;
            double positiveAffinity = affinity(embeddings, nodeOffset, positiveNodeOffset);
            double negativeAffinity = affinity(embeddings, nodeOffset, negativeNodeOffset);

            double positiveLogistic = logisticFunction(positiveAffinity);
            double negativeLogistic = logisticFunction(-negativeAffinity);

            IntStream.range(0, embeddingDimension).forEach(embeddingIdx -> computeGradientForEmbeddingIdx(
                embeddings,
                gradientResult,
                nodeOffset,
                positiveNodeOffset,
                negativeNodeOffset,
                positiveLogistic,
                negativeLogistic,
                embeddingIdx
            ));

        });
        return gradientResult;
    }

    private void computeGradientForEmbeddingIdx(
        Tensor<?> embeddings,
        Matrix gradientResult,
        int nodeOffset,
        int positiveNodeOffset,
        int negativeNodeOffset,
        double positiveLogistic,
        double negativeLogistic,
        int embeddingIdx
    ) {
        var embeddingLength = embeddings.dimension(COLUMNS_INDEX);
        int nodeIndex = nodeOffset * embeddingLength + embeddingIdx;
        int positiveNodeIndex = positiveNodeOffset * embeddingLength + embeddingIdx;
        int negativeNodeIndex = negativeNodeOffset * embeddingLength + embeddingIdx;

        double relationshipWeightFactor = relationshipWeightFactor(batch[nodeOffset], batch[positiveNodeOffset]);
        double weightedPositiveLogistic = relationshipWeightFactor * positiveLogistic;

        double scaledPositiveExampleGradient = -embeddings.dataAt(positiveNodeIndex) * weightedPositiveLogistic;
        double scaledNegativeExampleGradient = negativeSamplingFactor * embeddings.dataAt(negativeNodeIndex) * negativeLogistic;
        gradientResult.setDataAt(nodeIndex, scaledPositiveExampleGradient + scaledNegativeExampleGradient);

        gradientResult.setDataAt(positiveNodeIndex, -embeddings.dataAt(nodeIndex) * weightedPositiveLogistic);
        gradientResult.setDataAt(negativeNodeIndex, negativeSamplingFactor * embeddings.dataAt(nodeIndex) * negativeLogistic);
    }

    private double logisticFunction(double affinity) {
        return 1 / (1 + Math.pow(Math.E, affinity));
    }
}
