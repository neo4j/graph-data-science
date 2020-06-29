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
package org.neo4j.gds.embeddings.graphsage;

import org.neo4j.gds.embeddings.graphsage.ddl4j.ComputationContext;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.SingleParentVariable;

import java.util.stream.IntStream;

public class GraphSageLoss extends SingleParentVariable {

    private static final int NEGATIVE_NODES_OFFSET = 2;

    private final Matrix combinedEmbeddings;
    private final int negativeSamplingFactor;

    GraphSageLoss(Matrix combinedEmbeddings, int negativeSamplingFactor) {
        super(combinedEmbeddings, Dimensions.scalar());
        this.combinedEmbeddings = combinedEmbeddings;
        this.negativeSamplingFactor = negativeSamplingFactor;
    }

    @Override
    public Tensor apply(ComputationContext ctx) {
        Tensor embeddingData = ctx.data(parents().get(0));
        int batchSize = embeddingData.dimension(0) / 3;
        double loss = IntStream.range(0, batchSize).mapToDouble(nodeId -> {
            int positiveNodeId = nodeId + batchSize;
            int negativeNodeId = nodeId + NEGATIVE_NODES_OFFSET * batchSize;
            double positiveAffinity = affinity(embeddingData, nodeId, positiveNodeId);
            double negativeAffinity = affinity(embeddingData, nodeId, negativeNodeId);
            return -Math.log(Sigmoid.sigmoid(positiveAffinity)) - negativeSamplingFactor * Math.log(Sigmoid.sigmoid(-negativeAffinity));
        }).sum();
        return Tensor.scalar(loss);
    }

    private double affinity(Tensor embeddingData, int nodeId, int otherNodeId) {
        int dimensionSize = combinedEmbeddings.cols();
        double sum = 0;
        for (int i = 0; i < dimensionSize; i++) {
            sum += embeddingData.data[nodeId * dimensionSize + i] * embeddingData.data[otherNodeId * dimensionSize + i];
        }
        return sum;
    }

    @Override
    public Tensor gradient(Variable parent, ComputationContext ctx) {
        Tensor embeddingData = ctx.data(parent);
        double[] embeddings = embeddingData.data;
        int totalBatchSize = embeddingData.dimension(0);
        int batchSize = totalBatchSize / 3;

        int embeddingSize = embeddingData.dimension(1);
        double[] gradientResult = new double[totalBatchSize * embeddingSize];

        IntStream.range(0, batchSize).forEach(nodeId -> {
            int positiveNodeId = nodeId + batchSize;
            int negativeNodeId = nodeId + NEGATIVE_NODES_OFFSET * batchSize;
            int dimension = parent.dimension(1);
            double positiveAffinity = affinity(embeddingData, nodeId, positiveNodeId);
            double negativeAffinity = affinity(embeddingData, nodeId, negativeNodeId);

            double positiveLogistic = logisticFunction(positiveAffinity);
            double negativeLogistic = logisticFunction(-negativeAffinity);

            IntStream.range(0, embeddingSize).forEach(columnOffset -> partialComputeGradient(
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
        return Tensor.matrix(gradientResult, totalBatchSize, embeddingSize);
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
        gradientResult[nodeIndex] = -embeddings[positiveNodeIndex] * positiveLogistic +
            negativeSamplingFactor * embeddings[negativeNodeIndex] * negativeLogistic;

        gradientResult[positiveNodeIndex] = -embeddings[nodeIndex] * positiveLogistic;

        gradientResult[negativeNodeIndex] = negativeSamplingFactor * embeddings[nodeIndex] * negativeLogistic;
    }

    private double logisticFunction(double affinity) {
        return 1 / (1 + Math.pow(Math.E, affinity));
    }
}
