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
import org.neo4j.gds.embeddings.graphsage.ddl4j.Tensor;
import org.neo4j.gds.embeddings.graphsage.ddl4j.Variable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.SingleParentVariable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.Sigmoid;

import java.util.stream.IntStream;

public class GraphSageLoss extends SingleParentVariable {
    private final Variable combinedEmbeddings;
    private final int Q;

    GraphSageLoss(Variable combinedEmbeddings, int Q) {
        super(combinedEmbeddings, Dimensions.scalar());
        this.combinedEmbeddings = combinedEmbeddings;
        this.Q = Q;
    }

    @Override
    protected Tensor apply(ComputationContext ctx) {
        Tensor embeddingData = ctx.data(parents.get(0));
        int batchSize = embeddingData.dimensions[0] / 3;
        double loss = IntStream.range(0, batchSize).mapToDouble(nodeId -> {
            int positiveNodeId = nodeId + batchSize;
            int negativeNodeId = nodeId + 2 * batchSize;
            double positiveAffinity = affinity(embeddingData, nodeId, positiveNodeId);
            double negativeAffinity = affinity(embeddingData, nodeId, negativeNodeId);
            return -Math.log(Sigmoid.sigmoid(positiveAffinity)) - Q * Math.log(Sigmoid.sigmoid(-negativeAffinity));
        }).sum();
        return Tensor.scalar(loss);
    }

    private double affinity(Tensor embeddingData, int nodeId, int otherNodeId) {
        int dimension = combinedEmbeddings.dimension(1);
        double sum = 0;
        for (int i = 0; i < dimension; i++) {
            sum += embeddingData.data[nodeId * dimension + i] * embeddingData.data[otherNodeId * dimension + i];
        }
        return sum;
    }

    @Override
    protected Tensor gradient(ComputationContext ctx) {
        Tensor embeddingData = ctx.data(parent);
        double[] embeddingArr = embeddingData.data;
        int totalBatchSize = embeddingData.dimensions[0];
        int batchSize = totalBatchSize / 3;

        int embeddingSize = embeddingData.dimensions[1];
        double[] gradientResult = new double[totalBatchSize * embeddingSize];

        IntStream.range(0, batchSize).forEach(nodeId -> {
            int positiveNodeId = nodeId + batchSize;
            int negativeNodeId = nodeId + 2 * batchSize;
            int dimension = parent.dimension(1);
            double positiveAffinity = affinity(embeddingData, nodeId, positiveNodeId);
            double negativeAffinity = affinity(embeddingData, nodeId, negativeNodeId);

            double positiveLogistic = logisticFunction(positiveAffinity);
            double negativeLogistic = logisticFunction(-negativeAffinity);

            IntStream.range(0, embeddingSize).forEach(columnOffset -> {
                gradientResult[nodeId * dimension + columnOffset] =
                    - embeddingArr[positiveNodeId * dimension + columnOffset] * positiveLogistic +
                    Q * embeddingArr[negativeNodeId * dimension + columnOffset] * negativeLogistic;

                gradientResult[positiveNodeId * dimension + columnOffset] =
                    - embeddingArr[nodeId * dimension + columnOffset] * positiveLogistic;

                gradientResult[negativeNodeId * dimension + columnOffset] =
                    Q * embeddingArr[nodeId * dimension + columnOffset] * negativeLogistic;
            });

        });
        return Tensor.matrix(gradientResult, totalBatchSize, embeddingSize);
    }

    private double logisticFunction(double positiveAffinity) {
        return 1 / (1 + Math.pow(Math.E, positiveAffinity));
    }


}
