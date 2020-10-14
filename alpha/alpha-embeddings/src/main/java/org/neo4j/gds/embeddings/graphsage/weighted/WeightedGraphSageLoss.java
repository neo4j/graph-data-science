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
package org.neo4j.gds.embeddings.graphsage.weighted;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.SingleParentVariable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;

import java.util.stream.IntStream;

import static org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions.ROWS_INDEX;

public class WeightedGraphSageLoss extends SingleParentVariable<Scalar> {

    private static final int NEGATIVE_NODES_OFFSET = 2;

    private final RelationshipWeightsFunction relationshipWeightsFunction;
    private final Variable<Matrix> combinedEmbeddings;
    private final int negativeSamplingFactor;

    // TODO: Pass this as configuration parameter.
    private final double alpha = 1d;

    WeightedGraphSageLoss(RelationshipWeightsFunction relationshipWeightsFunction, Variable<Matrix> combinedEmbeddings, int negativeSamplingFactor) {
        super(combinedEmbeddings, Dimensions.scalar());
        this.relationshipWeightsFunction = relationshipWeightsFunction;
        this.combinedEmbeddings = combinedEmbeddings;
        this.negativeSamplingFactor = negativeSamplingFactor;
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        Tensor<?> embeddingData = ctx.data(parent());
        int batchSize = embeddingData.dimension(ROWS_INDEX) / 3;
        double loss = IntStream.range(0, batchSize).mapToDouble(nodeId -> {
            int positiveNodeId = nodeId + batchSize;
            int negativeNodeId = nodeId + NEGATIVE_NODES_OFFSET * batchSize;
            double positiveAffinity = affinity(embeddingData, nodeId, positiveNodeId);
            double negativeAffinity = affinity(embeddingData, nodeId, negativeNodeId);

            return -relationshipWeightFactor(nodeId, positiveNodeId) * Math.log(Sigmoid.sigmoid(positiveAffinity))
                   - negativeSamplingFactor * Math.log(Sigmoid.sigmoid(-negativeAffinity));
        }).sum();
        return new Scalar(loss);
    }

    private double relationshipWeightFactor(int nodeId, int positiveNodeId) {
        double relationshipWeight = relationshipWeightsFunction.apply((long) nodeId, (long) positiveNodeId, 1d);
        if(Double.isNaN(relationshipWeight)) {
            relationshipWeight = 1d;
        }
        return Math.pow(relationshipWeight, alpha);
    }

    private double affinity(Tensor<?> embeddingData, int nodeId, int otherNodeId) {
        int dimensionSize = combinedEmbeddings.dimension(COLUMNS_INDEX);
        double sum = 0;
        for (int i = 0; i < dimensionSize; i++) {
            sum += embeddingData.dataAt(nodeId * dimensionSize + i) * embeddingData.dataAt(otherNodeId * dimensionSize + i);
        }
        return sum;
    }

    @Override
    public Matrix gradient(Variable<?> parent, ComputationContext ctx) {
        Tensor<?> embeddingData = ctx.data(parent);
        double[] embeddings = embeddingData.data();
        int totalBatchSize = embeddingData.dimension(ROWS_INDEX);
        int batchSize = totalBatchSize / 3;

        int embeddingDimension = embeddingData.dimension(COLUMNS_INDEX);
        double[] gradientResult = new double[totalBatchSize * embeddingDimension];

        IntStream.range(0, batchSize).forEach(nodeId -> {
            int positiveNodeId = nodeId + batchSize;
            int negativeNodeId = nodeId + NEGATIVE_NODES_OFFSET * batchSize;
            int dimension = parent.dimension(COLUMNS_INDEX);
            double positiveAffinity = affinity(embeddingData, nodeId, positiveNodeId);
            double negativeAffinity = affinity(embeddingData, nodeId, negativeNodeId);

            double positiveLogistic = logisticFunction(positiveAffinity);
            double negativeLogistic = logisticFunction(-negativeAffinity);

            IntStream.range(0, embeddingDimension).forEach(columnOffset -> partialComputeGradient(
                embeddings,
                gradientResult,
                nodeId,
                positiveNodeId,
                negativeNodeId,
                dimension,
                positiveLogistic,
                negativeLogistic,
                columnOffset
            ));

        });
        return new Matrix(gradientResult, totalBatchSize, embeddingDimension);
    }

    private void partialComputeGradient(
        double[] embeddings,
        double[] gradientResult,
        int nodeId,
        int positiveNodeId,
        int negativeNodeId,
        int dimension,
        double positiveLogistic,
        double negativeLogistic,
        int columnOffset
    ) {
        int nodeIndex = nodeId * dimension + columnOffset;
        int positiveNodeIndex = positiveNodeId * dimension + columnOffset;
        int negativeNodeIndex = negativeNodeId * dimension + columnOffset;

        double relationshipWeightFactor = relationshipWeightFactor(nodeId, positiveNodeId);

        gradientResult[nodeIndex] = -embeddings[positiveNodeIndex] * relationshipWeightFactor * positiveLogistic +
            negativeSamplingFactor * embeddings[negativeNodeIndex] * negativeLogistic;

        gradientResult[positiveNodeIndex] = -embeddings[nodeIndex] * relationshipWeightFactor * positiveLogistic;

        gradientResult[negativeNodeIndex] = negativeSamplingFactor * embeddings[nodeIndex] * negativeLogistic;
    }

    private double logisticFunction(double affinity) {
        return 1 / (1 + Math.pow(Math.E, affinity));
    }
}
