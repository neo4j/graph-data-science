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
import org.neo4j.gds.embeddings.graphsage.ddl4j.functions.SingleParentVariable;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Matrix;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Scalar;
import org.neo4j.gds.embeddings.graphsage.ddl4j.tensor.Tensor;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions.COLUMNS_INDEX;
import static org.neo4j.gds.embeddings.graphsage.ddl4j.Dimensions.ROWS_INDEX;

public class HingeLoss extends SingleParentVariable<Scalar> {

    private static final int NEGATIVE_NODES_OFFSET = 2;

    private double loss;
    private final Variable<Matrix> combinedEmbeddings;

    HingeLoss(Variable<Matrix> combinedEmbeddings) {
        super(combinedEmbeddings, Dimensions.scalar());
        // 3 * batch size
        // 1 * current nodes
        // 1 * positive nodes
        // 1 * negative nodes
        this.combinedEmbeddings = combinedEmbeddings;
    }

    @Override
    public Scalar apply(ComputationContext ctx) {
        Tensor<?> embeddingData = ctx.data(parent());
        int batchSize = embeddingData.dimension(ROWS_INDEX) / 3;

        var data = embeddingData.data();
        var cols = combinedEmbeddings.dimension(COLUMNS_INDEX);
        loss = IntStream.range(0, batchSize).mapToDouble(nodeId -> {
            int positiveNodeId = nodeId + batchSize;
            int negativeNodeId = nodeId + NEGATIVE_NODES_OFFSET * batchSize;

            var currentNodeEmbeddings = Arrays.copyOfRange(data, nodeId, nodeId + cols);
            var positiveNeighborEmbeddings = Arrays.copyOfRange(data, positiveNodeId, positiveNodeId + cols);
            var negativeNeighborEmbeddingsEmbeddings = Arrays.copyOfRange(data, negativeNodeId, negativeNodeId + cols);

            var positiveMultiplied = 0;
            var negativeMultiplied = 0;
            for(int i = 0; i < cols; i++) {
                double currentNodeEmbedding = currentNodeEmbeddings[i];
                positiveMultiplied += currentNodeEmbedding * positiveNeighborEmbeddings[i];
                negativeMultiplied += currentNodeEmbedding * negativeNeighborEmbeddingsEmbeddings[i];
            }

            return Math.max(0, -positiveMultiplied + negativeMultiplied);
        }).sum();
        return new Scalar(loss);
    }

    @Override
    public Matrix gradient(Variable<?> parent, ComputationContext ctx) {
        Tensor<?> parentData = ctx.data(parent);
        Tensor<?> gradient = parentData.scalarMultiply(loss);
        int totalBatchSize = parentData.dimension(ROWS_INDEX);
        int embeddingDimension = parentData.dimension(COLUMNS_INDEX);

        return new Matrix(gradient.data(), totalBatchSize, embeddingDimension);
    }

}
